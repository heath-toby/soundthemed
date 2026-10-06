//! Niri compositor event monitoring.
//!
//! Watches `niri msg --json event-stream` for:
//!   - Window urgency changes (bell) — plays "bell" when a window becomes urgent
//!   - Windows opening, closing and gaining focus
//!   - Programs opening their first window / closing their last one
//!   - Screenshots taken with niri's built-in screenshot action

use serde_json::Value;
use soundthemed_shared::sound_ids::SoundEvent;
use std::collections::{HashMap, HashSet};
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::process::Command;
use tokio::sync::mpsc;
use tokio::time::{Duration, Instant};

/// Focus moving because a window opened or closed is part of that
/// event, not a separate switch.
const SWITCH_QUIET: Duration = Duration::from_millis(500);

/// Start watching Niri compositor events. `windows` enables the
/// window and screenshot sounds; the bell is always on.
pub async fn watch(tx: mpsc::Sender<SoundEvent>, windows: bool) {
    if let Err(e) = watch_inner(tx, windows).await {
        log::error!("niri monitor error: {e}");
    }
}

fn window_id(v: &Value) -> Option<u64> {
    v["id"].as_u64()
}

/// Which program a window belongs to: its process, or failing that
/// its app ID (or the window itself).
fn program_key(window: &Value) -> String {
    match (window["pid"].as_u64(), window["app_id"].as_str()) {
        (Some(pid), _) => format!("pid:{pid}"),
        (None, Some(app_id)) => format!("app:{app_id}"),
        (None, None) => format!("window:{}", window["id"]),
    }
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

    /// Forget a window; returns (window was open, its program has no windows left).
    fn close(&mut self, id: u64) -> (bool, bool) {
        match self.program_of.remove(&id) {
            Some(program) => (true, self.program_window_count(&program) == 0),
            None => (false, false),
        }
    }
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

    let mut open_windows = Windows::default();
    let mut urgent_windows: HashSet<u64> = HashSet::new();
    let mut focused: Option<u64> = None;
    // niri starts with a full WindowsChanged dump; nothing before
    // that is a real change.
    let mut seeded = false;
    let mut last_open_close = Instant::now() - SWITCH_QUIET;

    while let Some(line) = lines.next_line().await? {
        let Ok(Value::Object(event)) = serde_json::from_str::<Value>(&line) else { continue };
        let Some((kind, body)) = event.iter().next() else { continue };

        let mut sounds: Vec<SoundEvent> = Vec::new();

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
                seeded = true;
            }
            "WindowOpenedOrChanged" => {
                let window = &body["window"];
                let Some(id) = window_id(window) else { continue };

                let (is_new, first_of_program) = open_windows.open(id, program_key(window));
                if is_new && seeded {
                    log::info!("niri: window {id} opened");
                    last_open_close = Instant::now();
                    if first_of_program {
                        sounds.push(SoundEvent::AppLaunched);
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
                let (was_open, last_of_program) = open_windows.close(id);
                if was_open {
                    log::info!("niri: window {id} closed");
                    last_open_close = Instant::now();
                    if last_of_program {
                        sounds.push(SoundEvent::AppClosed);
                    }
                    sounds.push(SoundEvent::WindowClose);
                }
            }
            "WindowFocusChanged" => {
                let id = window_id(body);
                let switched = seeded
                    && id.is_some()
                    && id != focused
                    && id.is_some_and(|id| open_windows.contains(id))
                    && last_open_close.elapsed() >= SWITCH_QUIET;
                focused = id;
                if switched {
                    sounds.push(SoundEvent::WindowSwitch);
                }
            }
            "ScreenshotCaptured" => {
                log::info!("niri: screenshot captured");
                sounds.push(SoundEvent::ScreenCapture);
            }
            _ => {}
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
