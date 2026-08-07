#![expect(
    clippy::disallowed_types,
    reason = "the gate owns one short-lived state lock; no lock is held across await"
)]

use std::sync::{Arc, Mutex};

use tokio::sync::Notify;

/// An exact operation rendezvous controlled by a test.
#[derive(Debug, Clone, Default)]
pub struct Gate {
    inner: Arc<GateInner>,
}

#[derive(Debug, Default)]
struct GateInner {
    state: Mutex<GateState>,
    arrival: Notify,
    release: Notify,
    finish: Notify,
}

#[derive(Debug, Default)]
struct GateState {
    arrived: bool,
    released: bool,
    finished: bool,
}

impl Gate {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Wait until the selected object-store operation reaches this gate.
    pub async fn wait_until_blocked(&self) {
        loop {
            let arrival = self.inner.arrival.notified();
            if self.state().arrived {
                return;
            }
            arrival.await;
        }
    }

    /// Let the selected operation continue. Repeated releases have no effect.
    pub fn release(&self) {
        let mut state = self.state();
        state.released = true;
        drop(state);
        self.inner.release.notify_waiters();
    }

    /// Wait until the selected operation reaches a terminal storage outcome or is cancelled.
    pub async fn wait_until_finished(&self) {
        loop {
            let finish = self.inner.finish.notified();
            if self.state().finished {
                return;
            }
            finish.await;
        }
    }

    pub(super) async fn block(&self) {
        {
            let mut state = self.state();
            state.arrived = true;
        }
        self.inner.arrival.notify_waiters();

        loop {
            let release = self.inner.release.notified();
            if self.state().released {
                return;
            }
            release.await;
        }
    }

    pub(super) fn finish(&self) {
        let mut state = self.state();
        state.finished = true;
        drop(state);
        self.inner.finish.notify_waiters();
    }

    fn state(&self) -> std::sync::MutexGuard<'_, GateState> {
        match self.inner.state.lock() {
            Ok(state) => state,
            Err(poisoned) => poisoned.into_inner(),
        }
    }
}
