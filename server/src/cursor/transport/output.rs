//! Buffers, replays, broadcasts, and atomically closes downstream output.

use bytes::Bytes;
use tokio::sync::mpsc;

#[derive(Default)]
pub struct OutputHub {
    state: parking_lot::Mutex<OutputState>,
}

#[derive(Default)]
struct OutputState {
    history: Vec<Bytes>,
    subscribers: Vec<mpsc::UnboundedSender<Bytes>>,
    run_sse_subscribers: usize,
    disconnecting: bool,
    closed: bool,
}

impl OutputHub {
    pub fn emit(&self, frame: Bytes) -> bool {
        let mut state = self.state.lock();
        if state.closed {
            return false;
        }
        state.history.push(frame.clone());
        state
            .subscribers
            .retain(|subscriber| subscriber.send(frame.clone()).is_ok());
        true
    }

    pub fn subscribe(&self) -> mpsc::UnboundedReceiver<Bytes> {
        self.state.lock().subscribe()
    }

    pub(crate) fn subscribe_run_sse(&self) -> Option<mpsc::UnboundedReceiver<Bytes>> {
        let mut state = self.state.lock();
        if state.disconnecting {
            return None;
        }
        state.run_sse_subscribers += 1;
        Some(state.subscribe())
    }

    pub(crate) fn unsubscribe_run_sse(&self, completed: bool) -> bool {
        let mut state = self.state.lock();
        state.run_sse_subscribers -= 1;
        if completed || state.closed || state.run_sse_subscribers != 0 {
            return false;
        }
        // Commit disconnect before queuing it so a later subscriber cannot join a cancelled channel.
        state.disconnecting = true;
        true
    }

    pub fn close(&self) -> bool {
        let mut state = self.state.lock();
        if state.closed {
            return false;
        }
        state.closed = true;
        state.subscribers.clear();
        true
    }
}

impl OutputState {
    fn subscribe(&mut self) -> mpsc::UnboundedReceiver<Bytes> {
        let (sender, receiver) = mpsc::unbounded_channel();
        for frame in &self.history {
            let _ = sender.send(frame.clone());
        }
        if !self.closed {
            self.subscribers.push(sender);
        }
        receiver
    }
}
