// SPDX-License-Identifier: AGPL-3.0-or-later
// SPDX-FileCopyrightText: 2026 Jareer Abdullah and Concat contributors

//! The API server's watchdog: a thread that proves the server answers,
//! and asks for a fresh one when it stops.
//!
//! The server can stop answering while the process is alive - a call
//! wedged in the hub, a listener gone - and a caller that times out
//! cannot tell that from a server that is merely busy. So a thread of
//! its own asks, every thirty seconds, for the server's version through
//! the front door a caller would use: a connection, the token, a call.
//! One failure is a server that is merely busy, so it is asked once more
//! before anything is restarted; two in a row ask the window for a fresh
//! server ([`SettingsMsg::ServerRestartNeeded`]), which the settings
//! sheet stops and starts again on a worker, as it does for a flip of
//! the switch. The probe cannot tell a wedged hub from a frozen process;
//! either way a fresh server is the answer, and the foreground service
//! is what keeps the process from freezing in the first place.
//!
//! The thread ends when it is stopped, or once it has asked for a
//! restart: a fresh server brings a fresh watchdog, and the old thread
//! has nothing left to watch.

use std::io::{BufRead, BufReader, Write};
use std::net::{SocketAddr, TcpStream};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

/// How often the server is asked whether it answers, in seconds.
const PROBE_EVERY_SECS: u64 = 30;
/// How long one probe step - the connect, the token, the call - may take.
const PROBE_TIMEOUT: Duration = Duration::from_secs(5);

/// The server's watchdog, while it runs: a thread that asks the server
/// whether it answers. Stopped by dropping the flag; the thread then
/// ends at its next look, and a fresh server brings a fresh watchdog.
pub struct Watchdog {
    stop: Arc<AtomicBool>,
}

impl Watchdog {
    /// Starts the thread, probing the server at `address` with `token`.
    pub fn start(address: SocketAddr, token: String) -> Watchdog {
        let stop = Arc::new(AtomicBool::new(false));
        let flag = Arc::clone(&stop);
        let started = std::thread::Builder::new()
            .name("concat server watchdog".into())
            .spawn(move || {
                loop {
                    // Thirty seconds in one-second looks, so a stop is
                    // noticed at once rather than at the next probe.
                    for _ in 0..PROBE_EVERY_SECS {
                        if flag.load(Ordering::SeqCst) {
                            return;
                        }
                        std::thread::sleep(Duration::from_secs(1));
                    }
                    if flag.load(Ordering::SeqCst) {
                        return;
                    }
                    if probe(address, &token).is_ok() {
                        continue;
                    }
                    // One failure is a server that is merely busy - a call
                    // in flight holds the hub - so it is asked once more
                    // before anything is restarted.
                    log::warn!("server watchdog: the server did not answer; asking once more");
                    std::thread::sleep(Duration::from_secs(5));
                    if flag.load(Ordering::SeqCst) {
                        return;
                    }
                    if probe(address, &token).is_ok() {
                        continue;
                    }
                    log::warn!(
                        "server watchdog: the server did not answer twice; asking for a fresh one"
                    );
                    crate::host::on_ui(|studio, _, _| {
                        studio.handle(crate::panes::Msg::Settings(
                            crate::panes::settings::SettingsMsg::ServerRestartNeeded,
                        ));
                    });
                    return;
                }
            });
        if let Err(error) = started {
            log::warn!("server watchdog: could not start its thread: {error}");
        }
        Watchdog { stop }
    }

    /// Stops the thread. It ends at its next look; a probe in flight
    /// finishes first, so this never waits on the window's thread.
    pub fn stop(self) {
        self.stop.store(true, Ordering::SeqCst);
    }
}

/// Asks the server at `address`, as a caller would: a connection, the
/// token, a call. Anything that does not come back as a result is a
/// failure - a refused connection, a timeout, a refusal, a line that is
/// not JSON.
fn probe(address: SocketAddr, token: &str) -> Result<(), String> {
    let mut stream = TcpStream::connect_timeout(&concat_server::loopback(address), PROBE_TIMEOUT)
        .map_err(|error| format!("could not connect: {error}"))?;
    stream
        .set_write_timeout(Some(PROBE_TIMEOUT))
        .map_err(|error| format!("could not set the write timeout: {error}"))?;
    stream
        .set_read_timeout(Some(PROBE_TIMEOUT))
        .map_err(|error| format!("could not set the read timeout: {error}"))?;
    let mut reader = BufReader::new(
        stream
            .try_clone()
            .map_err(|error| format!("could not read: {error}"))?,
    );
    let auth = serde_json::json!({
        "jsonrpc": "2.0", "id": 1, "method": "auth", "params": { "token": token },
    });
    writeln!(stream, "{auth}").map_err(|error| format!("could not send the token: {error}"))?;
    let reply = read_reply(&mut reader, "the token")?;
    if reply.get("error").is_some() {
        return Err(format!("the token was refused: {}", reply["error"]));
    }
    if reply.get("result").is_none() {
        return Err(format!("the token's answer was not a result: {reply}"));
    }
    let version = serde_json::json!({
        "jsonrpc": "2.0", "id": 2, "method": "version",
    });
    writeln!(stream, "{version}")
        .map_err(|error| format!("could not ask for the version: {error}"))?;
    let reply = read_reply(&mut reader, "the version")?;
    if reply.get("error").is_some() {
        return Err(format!("the version was refused: {}", reply["error"]));
    }
    if reply.get("result").is_none() {
        return Err(format!("the version's answer was not a result: {reply}"));
    }
    Ok(())
}

/// One line back from the server, as JSON.
fn read_reply(reader: &mut BufReader<TcpStream>, what: &str) -> Result<serde_json::Value, String> {
    let mut line = String::new();
    reader
        .read_line(&mut line)
        .map_err(|error| format!("could not read {what}: {error}"))?;
    serde_json::from_str(&line).map_err(|error| format!("{what} was not JSON: {error}"))
}
