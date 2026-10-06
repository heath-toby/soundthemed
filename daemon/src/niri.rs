//! Niri compositor event monitoring.
//!
//! Watches `niri msg --json event-stream` for:
//!   - Window urgency changes (bell) — plays "bell" when a window becomes urgent
//!   - Windows opening, closing and gaining focus
//!   - Programs opening their first window, and closing: when a
//!     program's last window closes, its process is watched (pidfd).
//!     If it exits, that's "app-closed"; if it keeps running (e.g. in
//!     the system tray), that's "app-hidden", with "app-closed" once
//!     it really exits and "app-unhidden" if a window comes back
//!   - Screenshots taken with niri's built-in screenshot action

use serde_json::Value;
use soundthemed_shared::sound_ids::SoundEvent;
use std::collections::{HashMap, HashSet};
use std::os::fd::{FromRawFd, OwnedFd};
use tokio::io::unix::AsyncFd;
use tokio::io::{AsyncBufReadExt, BufReader, Interest};
use tokio::process::Command;
use tokio::sync::mpsc;
use tokio::time::{self, Duration, Instant};

/// Focus moving because a window opened or closed is part of that
/// event, not a separate switch.
const SWITCH_QUIET: Duration = Duration::from_millis(500);

/// niri reports focus moving off a closing window before the close
/// itself, so a switch is held this long in case that close follows.
const SWITCH_HOLD: Duration = Duration::from_millis(150);

/// After a program's last window closes, how long its process gets to
/// exit before it counts as still running in the background.
const QUIT_GRACE: Duration = Duration::from_secs(2);

/// Start watching Niri compositor events. `windows` enables the
/// window, program and screenshot sounds; the bell is always on.
pub async fn watch(tx: mpsc::Sender<SoundEvent>, windows: bool) {
    if let Err(e) = watch_inner(tx, windows).await {
        log::error!("niri monitor error: {e}");
    }
}

fn window_id(v: &Value) -> Option<u64> {
    v["id"].as_u64()
}

/// X11 windows all belong to the Xwayland bridge process, which never
/// exits, so those are told apart by app ID instead.
fn is_xwayland_bridge(pid: u64) -> bool {
    std::fs::read_to_string(format!("/proc/{pid}/comm"))
        .is_ok_and(|comm| comm.trim_start().to_lowercase().starts_with("xwayland"))
}

/// Which program a window belongs to: its process, or failing that
/// its app ID (or the window itself).
fn program_key(window: &Value) -> String {
    let pid = window["pid"].as_u64().filter(|&pid| !is_xwayland_bridge(pid));
    match (pid, window["app_id"].as_str()) {
        (Some(pid), _) => format!("pid:{pid}"),
        (None, Some(app_id)) => format!("app:{app_id}"),
        (None, None) => format!("window:{}", window["id"]),
    }
}

fn pid_of(program: &str) -> Option<i32> {
    program.strip_prefix("pid:")?.parse().ok()
}

/// Open windows and the program each belongs to.
#[derive(Default)]
struct Windows {
    program_of: HashMap<u64, String>,
}

impl Windows {
    fn contains(&self, id: u64) -> bool {
        self.program_of.contains_key(&id)
    }

    fn program_window_count(&self, program: &str) -> usize {
        self.program_of.values().filter(|p| *p == program).count()
    }

    /// Record a window; returns (window is new, its program had no windows).
    fn open(&mut self, id: u64, program: String) -> (bool, bool) {
        if self.contains(id) {
            return (false, false);
        }
        let first_of_program = self.program_window_count(&program) == 0;
        self.program_of.insert(id, program);
        (true, first_of_program)
    }

    /// Forget a window; returns its program if that was its last window.
    fn close(&mut self, id: u64) -> Option<Option<String>> {
        let program = self.program_of.remove(&id)?;
        let last = self.program_window_count(&program) == 0;
        Some(last.then_some(program))
    }
}

enum ProcessNews {
    /// Still alive QUIT_GRACE after its last window closed
    StillRunning,
    Exited,
}

/// A report from a process watcher. `generation` identifies the
/// watcher, so reports from one superseded by a newer close are ignored.
struct ProcessUpdate {
    program: String,
    generation: u64,
    news: ProcessNews,
}

fn open_pidfd(pid: i32) -> Option<AsyncFd<OwnedFd>> {
    let fd = unsafe { libc::syscall(libc::SYS_pidfd_open, pid, 0) };
    if fd < 0 {
        return None;
    }
    let fd = unsafe { OwnedFd::from_raw_fd(fd as i32) };
    AsyncFd::with_interest(fd, Interest::READABLE).ok()
}

/// Watch a process whose last window just closed. A pidfd becomes
/// readable when the process exits.
fn watch_process(
    pid: i32,
    program: String,
    generation: u64,
    updates: mpsc::UnboundedSender<ProcessUpdate>,
) {
    tokio::spawn(async move {
        let send = |news| {
            let _ = updates.send(ProcessUpdate {
                program: program.clone(),
                generation,
                news,
            });
        };

        // Can't watch it (already gone): count it as exited
        let Some(pidfd) = open_pidfd(pid) else {
            send(ProcessNews::Exited);
            return;
        };

        if time::timeout(QUIT_GRACE, pidfd.readable()).await.is_ok() {
            send(ProcessNews::Exited);
            return;
        }
        send(ProcessNews::StillRunning);

        let _ = pidfd.readable().await;
        send(ProcessNews::Exited);
    });
}

async fn watch_inner(
    tx: mpsc::Sender<SoundEvent>,
    windows: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    // XDG_CURRENT_DESKTOP can be "niri:GNOME", so go by niri's own socket
    if std::env::var_os("NIRI_SOCKET").is_none() {
        log::info!("niri: not running under niri, skipping");
        return Ok(());
    }

    let mut child = Command::new("niri")
        .args(["msg", "--json", "event-stream"])
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::null())
        .kill_on_drop(true)
        .spawn()?;

    log::info!("niri: watching compositor events");

    let stdout = child.stdout.take().ok_or("no stdout from niri msg")?;
    let mut lines = BufReader::new(stdout).lines();

    let (process_tx, mut process_rx) = mpsc::unbounded_channel::<ProcessUpdate>();

    let mut open_windows = Windows::default();
    let mut urgent_windows: HashSet<u64> = HashSet::new();
    let mut focused: Option<u64> = None;
    // niri starts with a full WindowsChanged dump; nothing before
    // that is a real change.
    let mut seeded = false;
    let mut last_open_close = Instant::now() - SWITCH_QUIET;
    // Programs with no windows whose process is being watched, by
    // watcher generation; `hidden` are the ones known to still run.
    let mut watching: HashMap<String, u64> = HashMap::new();
    let mut hidden: HashSet<String> = HashSet::new();
    let mut generation: u64 = 0;
    // A window switch waiting out SWITCH_HOLD: (when to play, window left)
    let mut pending_switch: Option<(Instant, Option<u64>)> = None;

    loop {
        let mut sounds: Vec<SoundEvent> = Vec::new();

        tokio::select! {
            line = lines.next_line() => {
                let Some(line) = line? else { break };
                let Ok(Value::Object(event)) = serde_json::from_str::<Value>(&line) else { continue };
                let Some((kind, body)) = event.iter().next() else { continue };

                match kind.as_str() {
                    "WindowsChanged" => {
                        let list = body["windows"].as_array().cloned().unwrap_or_default();
                        open_windows = Windows::default();
                        for window in &list {
                            if let Some(id) = window_id(window) {
                                open_windows.open(id, program_key(window));
                            }
                        }
                        urgent_windows = list
                            .iter()
                            .filter(|w| w["is_urgent"] == true)
                            .filter_map(window_id)
                            .collect();
                        focused = list.iter().find(|w| w["is_focused"] == true).and_then(window_id);
                        watching.clear();
                        hidden.clear();
                        seeded = true;
                    }
                    "WindowOpenedOrChanged" => {
                        let window = &body["window"];
                        let Some(id) = window_id(window) else { continue };
                        let program = program_key(window);

                        let (is_new, first_of_program) = open_windows.open(id, program.clone());
                        if is_new && seeded {
                            log::info!("niri: window {id} opened");
                            last_open_close = Instant::now();
                            if first_of_program {
                                if watching.remove(&program).is_none() {
                                    sounds.push(SoundEvent::AppLaunched);
                                } else if hidden.remove(&program) {
                                    log::info!("niri: {program} back from the background");
                                    sounds.push(SoundEvent::AppUnhidden);
                                }
                                // else: a window replaced within QUIT_GRACE; stay quiet
                            }
                            sounds.push(SoundEvent::WindowNew);
                        }
                        if window["is_focused"] == true {
                            focused = Some(id);
                        }
                        if window["is_urgent"] == true {
                            if urgent_windows.insert(id) {
                                log::info!("niri: window {id} became urgent (bell)");
                                sounds.push(SoundEvent::Custom("bell".into()));
                            }
                        } else {
                            urgent_windows.remove(&id);
                        }
                    }
                    "WindowUrgencyChanged" => {
                        let Some(id) = window_id(body) else { continue };
                        if body["urgent"] == true {
                            if urgent_windows.insert(id) {
                                log::info!("niri: window {id} became urgent (bell)");
                                sounds.push(SoundEvent::Custom("bell".into()));
                            }
                        } else {
                            urgent_windows.remove(&id);
                        }
                    }
                    "WindowClosed" => {
                        let Some(id) = window_id(body) else { continue };
                        urgent_windows.remove(&id);
                        if pending_switch.is_some_and(|(_, left)| left == Some(id)) {
                            // Focus only moved because this window closed
                            pending_switch = None;
                        }
                        let Some(last_of) = open_windows.close(id) else { continue };

                        log::info!("niri: window {id} closed");
                        last_open_close = Instant::now();
                        match last_of.as_deref().map(|p| (p, pid_of(p))) {
                            Some((program, Some(pid))) => {
                                // Wait to see whether the process exits
                                generation += 1;
                                watching.insert(program.to_owned(), generation);
                                watch_process(pid, program.to_owned(), generation, process_tx.clone());
                            }
                            Some((_, None)) => sounds.push(SoundEvent::AppClosed),
                            None => {}
                        }
                        sounds.push(SoundEvent::WindowClose);
                    }
                    "WindowFocusChanged" => {
                        let id = window_id(body);
                        let switched = seeded
                            && id.is_some()
                            && id != focused
                            && id.is_some_and(|id| open_windows.contains(id))
                            && last_open_close.elapsed() >= SWITCH_QUIET;
                        let left = std::mem::replace(&mut focused, id);
                        if switched {
                            pending_switch = Some((Instant::now() + SWITCH_HOLD, left));
                        }
                    }
                    "ScreenshotCaptured" => {
                        log::info!("niri: screenshot captured");
                        sounds.push(SoundEvent::ScreenCapture);
                    }
                    _ => {}
                }
            }
            _ = time::sleep_until(pending_switch.map_or_else(Instant::now, |(at, _)| at)), if pending_switch.is_some() => {
                pending_switch = None;
                sounds.push(SoundEvent::WindowSwitch);
            }
            Some(update) = process_rx.recv() => {
                if watching.get(&update.program) != Some(&update.generation) {
                    continue;
                }
                match update.news {
                    ProcessNews::StillRunning => {
                        log::info!("niri: {} still running with no windows", update.program);
                        hidden.insert(update.program);
                        sounds.push(SoundEvent::AppHidden);
                    }
                    ProcessNews::Exited => {
                        log::info!("niri: {} exited", update.program);
                        watching.remove(&update.program);
                        hidden.remove(&update.program);
                        sounds.push(SoundEvent::AppClosed);
                    }
                }
            }
        }

        for sound in sounds {
            let is_bell = matches!(&sound, SoundEvent::Custom(id) if id == "bell");
            if !windows && !is_bell {
                continue;
            }
            if tx.send(sound).await.is_err() {
                return Ok(());
            }
        }
    }

    Ok(())
}
