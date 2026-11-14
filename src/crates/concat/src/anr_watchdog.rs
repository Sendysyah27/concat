// SPDX-License-Identifier: AGPL-3.0-or-later
// SPDX-FileCopyrightText: 2026 Jareer Abdullah and Concat contributors

//! The main thread's watchdog: proof, when the window stops answering,
//! of where it was and for how long.
//!
//! An ANR is the window's event loop not answering the system for five
//! seconds, and the system only says that it happened - not what the loop
//! was in the middle of. This module watches for exactly that: a timer on
//! the loop proves it is alive twice a second, and a thread of its own
//! notices when the proof stops coming. When it does, the thread writes
//! down what the loop was doing last - the action it was handed, and the
//! stack it was standing on a moment before - into the logs folder, logs
//! the same, and on a phone hands a copy to the file manager, so the next
//! report of "it froze" comes with the place it froze in.
//!
//! Nothing here changes what the window does. The timer's tick is a
//! backtrace capture and a counter, twice a second, and the thread only
//! wakes to read the counter. The one thing it writes is the dump, and
//! only when the loop has already been silent for five seconds.
//!
//! A live sample of the loop's stack is as close as this gets: Rust has
//! no way to unwind another thread's stack, and stopping the loop with a
//! signal to take one would risk the very freeze it is meant to report.
//! What the dump holds instead is the loop's own stack from its last
//! tick, at most half a second before it went silent - the call it was
//! in, not the one it was interrupted in - and the action it was handed,
//! which names the work even when the binary's names are stripped.

use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, Instant, SystemTime};

/// How often the loop proves it is alive, in milliseconds.
const HEARTBEAT_MS: u64 = 500;
/// How long the proof may be silent before the loop counts as stuck, in
/// milliseconds: the system's own ANR line.
const STALL_AFTER_MS: u64 = 5_000;
/// How many of the watchdog's own files are kept in the logs folder.
const KEEP_DUMPS: usize = 5;

/// What the timer and the thread share.
struct State {
    /// The loop's own clock: milliseconds since this started, written by
    /// the timer's tick and read by the thread.
    heartbeat_ms: AtomicU64,
    /// The loop's stack from its last tick, with when it was taken, in
    /// milliseconds since this started.
    last_sample: Mutex<Option<(std::backtrace::Backtrace, u64)>>,
    /// The last action the loop was handed, with when, in milliseconds
    /// since this started.
    last_action: Mutex<Option<(&'static str, u64)>>,
    /// Where a dump goes, when there is one: the logs folder.
    logs_folder: Option<PathBuf>,
    /// The clock the heartbeat is read against.
    started_at: Instant,
}

impl State {
    fn new() -> Self {
        let logs_folder = concat_host::AppDirs::locate()
            .ok()
            .map(|dirs| concat_host::logs::folder(&dirs));
        State {
            heartbeat_ms: AtomicU64::new(0),
            last_sample: Mutex::new(None),
            last_action: Mutex::new(None),
            logs_folder,
            started_at: Instant::now(),
        }
    }

    /// Milliseconds since this started.
    fn elapsed_ms(&self) -> u64 {
        self.started_at.elapsed().as_millis() as u64
    }
}

static STATE: OnceLock<Arc<State>> = OnceLock::new();
static STARTED: AtomicBool = AtomicBool::new(false);

/// The shared state, made on first use: the timer and the thread both
/// start from it, and [`note_action`] before either is no less recorded.
fn state() -> &'static Arc<State> {
    STATE.get_or_init(|| Arc::new(State::new()))
}

/// Starts the watchdog: the timer on the loop and the thread that watches
/// it. Once; a second call hands back a timer that does nothing. The
/// timer returned is the loop's proof of life: hold it until the loop
/// ends, as the reader sweep's is held, or the proof stops with it.
///
/// Call after the window exists and before the loop is entered. A smoke
/// run leaves before the loop, past this point, so it never starts one.
/// The thread writes nothing until the loop has been silent for
/// [`STALL_AFTER_MS`].
pub fn start() -> slint::Timer {
    let timer = slint::Timer::default();
    if STARTED.swap(true, Ordering::SeqCst) {
        return timer;
    }
    let shared = Arc::clone(state());
    let tick = Arc::clone(&shared);
    timer.start(
        slint::TimerMode::Repeated,
        Duration::from_millis(HEARTBEAT_MS),
        move || {
            tick.heartbeat_ms
                .store(tick.elapsed_ms(), Ordering::Relaxed);
            let sample = std::backtrace::Backtrace::force_capture();
            if let Ok(mut slot) = tick.last_sample.lock() {
                *slot = Some((sample, tick.elapsed_ms()));
            }
        },
    );
    let watcher = Arc::clone(&shared);
    let spawned = std::thread::Builder::new()
        .name("anr-watchdog".to_owned())
        .spawn(move || watch(watcher))
        .is_ok();
    if !spawned {
        log::warn!("anr-watchdog: the watching thread could not start");
    }
    timer
}

/// Names the work the loop was just handed, so a dump says what it was
/// doing in words even when the binary's names are stripped. Cheap, and
/// safe to call from any thread; a call before [`start`] still records.
/// The server's own actions are the ones named, in `settings.rs`: they
/// are the work that froze the window once.
pub fn note_action(action: &'static str) {
    let shared = state();
    if let Ok(mut slot) = shared.last_action.lock() {
        *slot = Some((action, shared.elapsed_ms()));
    }
}

/// The thread: reads the heartbeat twice a second, and writes the dump
/// once per silence, with a line when the loop answers again.
fn watch(state: Arc<State>) {
    let mut stalled: Option<(u64, Option<PathBuf>)> = None;
    loop {
        std::thread::sleep(Duration::from_millis(HEARTBEAT_MS));
        let now_ms = state.elapsed_ms();
        let age_ms = now_ms.saturating_sub(state.heartbeat_ms.load(Ordering::Relaxed));
        if age_ms > STALL_AFTER_MS {
            if stalled.is_none() {
                let path = write_dump(&state, age_ms, now_ms);
                log::warn!(
                    "anr-watchdog: main thread stalled for {age_ms}ms; dump: {}",
                    describe(&path)
                );
                stalled = Some((now_ms, path));
            }
            continue;
        }
        if let Some((started_ms, path)) = stalled.take() {
            let total_ms = now_ms.saturating_sub(started_ms);
            if let Some(path) = &path {
                append_recovery(path, total_ms);
            }
            log::warn!(
                "anr-watchdog: main thread answered again after {total_ms}ms; dump: {}",
                describe(&path)
            );
        }
    }
}

/// Where a dump is, in words, when there may be none.
fn describe(path: &Option<PathBuf>) -> String {
    path.as_ref()
        .map_or_else(|| "no file".to_owned(), |path| path.display().to_string())
}

/// Writes the dump for a silence, and hands a copy to the phone's file
/// manager where there is one. `None` when there is nowhere to write.
fn write_dump(state: &State, age_ms: u64, now_ms: u64) -> Option<PathBuf> {
    let folder = state.logs_folder.clone()?;
    let _ = std::fs::create_dir_all(&folder);
    let path = folder.join(format!("anr-watchdog-{}.txt", utc_stamp(SystemTime::now())));
    let text = dump_text(state, age_ms, now_ms);
    if std::fs::write(&path, &text).is_err() {
        return None;
    }
    prune(&folder);
    log::warn!("{text}");
    // A copy where the phone's file manager shows it, so the dump is
    // reachable without a cable: Android's Download/Concat, through the
    // same publisher a finished subtitle takes. Nothing on a desktop,
    // where the file above is already where the person chose.
    let published =
        crate::platform::publish_document(path.clone(), "text/plain", |result| match result {
            Ok(where_) => log::info!("anr-watchdog: a copy of the dump is at {where_}"),
            Err(error) => log::warn!("anr-watchdog: the dump's copy was not published: {error}"),
        });
    if !published {
        log::info!("anr-watchdog: dump written to {}", path.display());
    }
    Some(path)
}

/// The dump itself: when, how long silent, what the loop was handed, the
/// stack from its last tick, and what the other threads are doing.
fn dump_text(state: &State, age_ms: u64, now_ms: u64) -> String {
    use std::fmt::Write as _;
    let mut text = String::new();
    let _ = writeln!(
        text,
        "ANR watchdog: the main thread has been silent for {age_ms}ms"
    );
    let _ = writeln!(
        text,
        "detected at {} ({}ms since start)",
        utc_stamp(SystemTime::now()),
        now_ms
    );
    match state.last_action.lock() {
        Ok(slot) => match *slot {
            Some((action, at_ms)) => {
                let _ = writeln!(
                    text,
                    "last action: {action} ({}ms ago)",
                    now_ms.saturating_sub(at_ms)
                );
            }
            None => {
                let _ = writeln!(text, "last action: none recorded");
            }
        },
        Err(_) => {
            let _ = writeln!(text, "last action: unreadable");
        }
    }
    let sample = match state.last_sample.lock() {
        Ok(slot) => (*slot).clone(),
        Err(_) => None,
    };
    match sample {
        Some((sample, at_ms)) => {
            let _ = writeln!(
                text,
                "main thread stack, sampled {}ms before detection:",
                now_ms.saturating_sub(at_ms)
            );
            let _ = writeln!(text, "{sample}");
        }
        None => {
            let _ = writeln!(text, "main thread stack: no sample yet");
        }
    }
    let threads = thread_list();
    if threads.is_empty() {
        let _ = writeln!(text, "other threads: not listed on this platform");
    } else {
        let _ = writeln!(text, "other threads:");
        for thread in threads {
            let _ = writeln!(text, "  {thread}");
        }
    }
    text
}

/// Adds the recovery line to a dump already written.
fn append_recovery(path: &std::path::Path, total_ms: u64) {
    use std::io::Write as _;
    if let Ok(mut file) = std::fs::OpenOptions::new().append(true).open(path) {
        let _ = writeln!(
            file,
            "recovered: the main thread answered again after {total_ms}ms"
        );
    }
}

/// Keeps the newest [`KEEP_DUMPS`] of the watchdog's files in the folder,
/// by name, which for this name is by time.
fn prune(folder: &std::path::Path) {
    let Ok(entries) = std::fs::read_dir(folder) else {
        return;
    };
    let mut ours: Vec<PathBuf> = entries
        .flatten()
        .map(|entry| entry.path())
        .filter(|path| {
            path.file_name()
                .and_then(|name| name.to_str())
                .is_some_and(|name| name.starts_with("anr-watchdog-") && name.ends_with(".txt"))
        })
        .collect();
    if ours.len() <= KEEP_DUMPS {
        return;
    }
    ours.sort();
    let doomed = ours.len() - KEEP_DUMPS;
    for path in ours.into_iter().take(doomed) {
        let _ = std::fs::remove_file(path);
    }
}

/// The process's threads, as the kernel names them: id, state, name.
/// Linux and Android only; elsewhere there is nothing to list without a
/// crate for it, and the dump says so instead.
#[cfg(any(target_os = "linux", target_os = "android"))]
fn thread_list() -> Vec<String> {
    let mut threads = Vec::new();
    let Ok(tasks) = std::fs::read_dir("/proc/self/task") else {
        return threads;
    };
    for entry in tasks.flatten() {
        let tid = entry.file_name().to_string_lossy().into_owned();
        let base = entry.path();
        let name = std::fs::read_to_string(base.join("comm"))
            .map_or_else(|_| "?".to_owned(), |name| name.trim().to_owned());
        let stat = std::fs::read_to_string(base.join("stat")).unwrap_or_default();
        let state = stat
            .rfind(')')
            .and_then(|at| stat[at + 1..].split_whitespace().next())
            .unwrap_or("?");
        threads.push(format!("tid {tid} [{state}] {name}"));
    }
    threads.sort();
    threads
}

#[cfg(not(any(target_os = "linux", target_os = "android")))]
fn thread_list() -> Vec<String> {
    Vec::new()
}

/// The instant as UTC, `2026-11-14T21-30-00Z`: dashes, not colons, so
/// the stamp is a file name on every platform.
fn utc_stamp(when: SystemTime) -> String {
    let secs = when
        .duration_since(SystemTime::UNIX_EPOCH)
        .map_or(0, |span| span.as_secs() as i64);
    let days = secs.div_euclid(86_400);
    let secs_of_day = secs.rem_euclid(86_400);
    let (year, month, day) = civil_from_days(days);
    format!(
        "{year:04}-{month:02}-{day:02}T{:02}-{:02}-{:02}Z",
        secs_of_day / 3_600,
        (secs_of_day / 60) % 60,
        secs_of_day % 60
    )
}

/// The civil date for a day count since the epoch, by Howard Hinnant's
/// algorithm: the days shift to the civil era, and the year, month and
/// day fall out of the era's arithmetic.
fn civil_from_days(days: i64) -> (i64, u32, u32) {
    let shifted = days + 719_468;
    let era = shifted.div_euclid(146_097);
    let day_of_era = shifted.rem_euclid(146_097) as u64;
    let year_of_era =
        (day_of_era - day_of_era / 1_460 + day_of_era / 36_524 - day_of_era / 146_096) / 365;
    let mut year = year_of_era as i64 + era * 400;
    let day_of_year = day_of_era - (365 * year_of_era + year_of_era / 4 - year_of_era / 100);
    let month_prime = (5 * day_of_year + 2) / 153;
    let day = (day_of_year - (153 * month_prime + 2) / 5 + 1) as u32;
    let month_prime = if month_prime < 10 {
        month_prime + 3
    } else {
        month_prime - 9
    };
    let month = month_prime as u32;
    if month <= 2 {
        year += 1;
    }
    (year, month, day)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn utc_stamp_names_known_instants() {
        assert_eq!(utc_stamp(SystemTime::UNIX_EPOCH), "1970-01-01T00-00-00Z");
        assert_eq!(
            utc_stamp(SystemTime::UNIX_EPOCH + Duration::from_secs(1_700_000_000)),
            "2023-11-14T22-13-20Z"
        );
        assert_eq!(
            utc_stamp(SystemTime::UNIX_EPOCH + Duration::from_secs(1_763_126_400)),
            "2025-11-14T13-20-00Z"
        );
    }

    #[test]
    fn note_action_records_what_the_loop_was_handed() {
        note_action("ServerTokenGenerated");
        let action = state().last_action.lock().map(|slot| *slot).unwrap_or(None);
        assert!(matches!(action, Some(("ServerTokenGenerated", _))));
    }

    #[cfg(any(target_os = "linux", target_os = "android"))]
    #[test]
    fn the_thread_list_names_threads() {
        let threads = thread_list();
        assert!(!threads.is_empty());
        assert!(threads.iter().all(|thread| thread.starts_with("tid ")));
    }
}
