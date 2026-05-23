//! Background thread that polls crossterm and forwards events to the async runtime.

use std::time::Duration;

use crossterm::event::Event;
use tokio::sync::mpsc;

pub(crate) fn spawn_crossterm_channel() -> mpsc::UnboundedReceiver<Event> {
    let (ev_tx, ev_rx) = mpsc::unbounded_channel::<Event>();
    std::thread::spawn(move || loop {
        if crossterm::event::poll(Duration::from_millis(120)).unwrap_or(false) {
            if let Ok(ev) = crossterm::event::read() {
                let _ = ev_tx.send(ev);
            }
        }
    });
    ev_rx
}
