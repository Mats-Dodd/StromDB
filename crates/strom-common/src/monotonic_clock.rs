//! The monotonic-clock capability seam used for writer pacing.

use std::fmt;
use std::future::{Future, pending};
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll, Waker};
use std::time::Duration;

/// Elapsed process time with no wall-clock meaning.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct MonotonicInstant(Duration);

impl MonotonicInstant {
    pub const ZERO: Self = Self(Duration::ZERO);

    const fn duration_since_origin(self) -> Duration {
        self.0
    }

    #[must_use]
    pub const fn saturating_add(self, duration: Duration) -> Self {
        Self(self.0.saturating_add(duration))
    }
}

impl From<Duration> for MonotonicInstant {
    fn from(duration: Duration) -> Self {
        Self(duration)
    }
}

/// A monotonic time source whose sleeps own their complete lifetime.
pub trait MonotonicClock: fmt::Debug + Send + Sync + 'static {
    #[must_use]
    fn now(&self) -> MonotonicInstant;

    fn sleep_until(
        &self,
        deadline: MonotonicInstant,
    ) -> Pin<Box<dyn Future<Output = ()> + Send + 'static>>;
}

/// Tokio's monotonic clock, rooted when the value is constructed.
#[derive(Clone, Copy, Debug)]
pub struct OsMonotonicClock {
    origin: tokio::time::Instant,
}

impl Default for OsMonotonicClock {
    fn default() -> Self {
        Self {
            origin: tokio::time::Instant::now(),
        }
    }
}

impl MonotonicClock for OsMonotonicClock {
    fn now(&self) -> MonotonicInstant {
        MonotonicInstant::from(self.origin.elapsed())
    }

    fn sleep_until(
        &self,
        deadline: MonotonicInstant,
    ) -> Pin<Box<dyn Future<Output = ()> + Send + 'static>> {
        let origin = self.origin;
        #[expect(
            clippy::disallowed_methods,
            reason = "OsMonotonicClock is the sanctioned production sleeper behind the \
                      MonotonicClock seam"
        )]
        Box::pin(async move {
            let Some(deadline) = origin.checked_add(deadline.duration_since_origin()) else {
                pending::<()>().await;
                return;
            };
            tokio::time::sleep_until(deadline).await;
        })
    }
}

#[expect(
    clippy::disallowed_types,
    reason = "the manual clock serializes time, sleeper registration, and advance behind one \
              lock; no guard crosses an await and wakers run only after the guard is dropped"
)]
type ManualStateLock = std::sync::Mutex<ManualState>;

/// A deterministic monotonic clock advanced explicitly by tests.
#[derive(Clone, Debug)]
pub struct ManualMonotonicClock {
    state: Arc<ManualStateLock>,
}

#[derive(Debug)]
struct ManualState {
    now: MonotonicInstant,
    sleeper_id_next: SleeperId,
    sleepers: Vec<Sleeper>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct SleeperId(u64);

#[derive(Debug)]
struct Sleeper {
    id: SleeperId,
    deadline: MonotonicInstant,
    waker: Option<Waker>,
}

impl ManualMonotonicClock {
    #[must_use]
    pub fn new(start: MonotonicInstant) -> Self {
        Self {
            state: Arc::new(ManualStateLock::new(ManualState {
                now: start,
                sleeper_id_next: SleeperId(0),
                sleepers: Vec::new(),
            })),
        }
    }

    /// Move time forward and wake every reached sleeper.
    ///
    /// # Panics
    ///
    /// Panics when the clock lock is poisoned.
    pub fn advance(&self, duration: Duration) {
        let due_wakers = {
            let mut state = self
                .state
                .lock()
                .expect("manual monotonic clock lock poisoned");
            state.now = state.now.saturating_add(duration);
            let now = state.now;
            let mut due_wakers = Vec::new();
            state.sleepers.retain_mut(|sleeper| {
                if sleeper.deadline <= now {
                    if let Some(waker) = sleeper.waker.take() {
                        due_wakers.push(waker);
                    }
                    false
                } else {
                    true
                }
            });
            due_wakers
        };
        for waker in due_wakers {
            waker.wake();
        }
    }

    /// Number of sleeps currently retained by the clock.
    ///
    /// # Panics
    ///
    /// Panics when the clock lock is poisoned.
    #[must_use]
    pub fn sleeper_count(&self) -> usize {
        self.state
            .lock()
            .expect("manual monotonic clock lock poisoned")
            .sleepers
            .len()
    }
}

impl MonotonicClock for ManualMonotonicClock {
    fn now(&self) -> MonotonicInstant {
        self.state
            .lock()
            .expect("manual monotonic clock lock poisoned")
            .now
    }

    fn sleep_until(
        &self,
        deadline: MonotonicInstant,
    ) -> Pin<Box<dyn Future<Output = ()> + Send + 'static>> {
        let mut state = self
            .state
            .lock()
            .expect("manual monotonic clock lock poisoned");
        let sleeper_id = state.sleeper_id_next;
        state.sleeper_id_next = SleeperId(
            sleeper_id
                .0
                .checked_add(1)
                .expect("a run registers fewer monotonic sleeps than a u64 can number"),
        );
        state.sleepers.push(Sleeper {
            id: sleeper_id,
            deadline,
            waker: None,
        });
        drop(state);
        Box::pin(ManualSleep {
            state: Arc::clone(&self.state),
            sleeper_id,
            deadline,
        })
    }
}

struct ManualSleep {
    state: Arc<ManualStateLock>,
    sleeper_id: SleeperId,
    deadline: MonotonicInstant,
}

impl Future for ManualSleep {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        let unpinned = self.get_mut();
        let mut state = unpinned
            .state
            .lock()
            .expect("manual monotonic clock lock poisoned");
        if state.now >= unpinned.deadline {
            state
                .sleepers
                .retain(|sleeper| sleeper.id != unpinned.sleeper_id);
            drop(state);
            Poll::Ready(())
        } else {
            let sleeper = state
                .sleepers
                .iter_mut()
                .find(|sleeper| sleeper.id == unpinned.sleeper_id)
                .expect("a pending monotonic sleeper stays registered until its deadline");
            sleeper.waker = Some(cx.waker().clone());
            drop(state);
            Poll::Pending
        }
    }
}

impl Drop for ManualSleep {
    fn drop(&mut self) {
        if let Ok(mut state) = self.state.lock() {
            state
                .sleepers
                .retain(|sleeper| sleeper.id != self.sleeper_id);
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::Ordering;
    use std::task::{Wake, Waker};

    use super::*;

    #[test]
    fn manual_sleep_uses_an_absolute_owned_deadline() {
        let clock = ManualMonotonicClock::new(MonotonicInstant::ZERO);
        let advancing_clock = clock.clone();
        let mut sleep =
            clock.sleep_until(MonotonicInstant::ZERO.saturating_add(Duration::from_millis(10)));
        drop(clock);
        let recorder = Arc::new(RecordingWake(WokenFlag::new(false)));
        let waker = Waker::from(Arc::clone(&recorder));
        let mut context = Context::from_waker(&waker);

        assert!(sleep.as_mut().poll(&mut context).is_pending());
        advancing_clock.advance(Duration::from_millis(10));
        assert!(recorder.0.load(Ordering::SeqCst));
        assert!(sleep.as_mut().poll(&mut context).is_ready());
    }

    #[expect(
        clippy::disallowed_types,
        reason = "the test-only wake flag has the wake callback as its single writer"
    )]
    type WokenFlag = std::sync::atomic::AtomicBool;

    struct RecordingWake(WokenFlag);

    impl Wake for RecordingWake {
        fn wake(self: Arc<Self>) {
            self.0.store(true, Ordering::SeqCst);
        }
    }
}
