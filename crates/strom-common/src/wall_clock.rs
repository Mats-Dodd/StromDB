//! The wall-clock capability seam (stromstyle §4).

use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll, Waker};
use std::time::Duration;

/// Nanoseconds since the Unix epoch. The pure core receives this as data.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Timestamp(u128);

impl Timestamp {
    pub const UNIX_EPOCH: Self = Self(0);

    #[must_use]
    pub const fn from_unix_nanos(nanos: u128) -> Self {
        Self(nanos)
    }

    #[must_use]
    pub const fn as_unix_nanos(self) -> u128 {
        self.0
    }

    /// Adds the duration, saturating at the representable upper bound.
    #[must_use]
    pub const fn saturating_add(self, duration: Duration) -> Self {
        Self(self.0.saturating_add(duration.as_nanos()))
    }
}

/// The wall-clock capability: production shells use [`OsClock`], deterministic
/// runs use [`ManualClock`], across this same seam.
pub trait Clock: fmt::Debug + Send + Sync {
    #[must_use]
    fn now(&self) -> Timestamp;

    /// Completes once `duration` has passed. Boxed to keep the trait object-safe.
    fn sleep(&self, duration: Duration) -> Pin<Box<dyn Future<Output = ()> + Send + '_>>;
}

/// Reads the operating system clock and sleeps on the runtime.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct OsClock;

impl Clock for OsClock {
    fn now(&self) -> Timestamp {
        #[expect(
            clippy::disallowed_methods,
            reason = "OsClock is the single sanctioned wall-clock read behind the Clock seam"
        )]
        let since_epoch = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("the OS clock reports a time before the Unix epoch");
        Timestamp::from_unix_nanos(since_epoch.as_nanos())
    }

    fn sleep(&self, duration: Duration) -> Pin<Box<dyn Future<Output = ()> + Send + '_>> {
        #[expect(
            clippy::disallowed_methods,
            reason = "OsClock is the single sanctioned production sleeper behind the Clock seam"
        )]
        let sleep_future = tokio::time::sleep(duration);
        Box::pin(sleep_future)
    }
}

#[expect(
    clippy::disallowed_types,
    reason = "ManualClock serializes time reads, sleeper registration, and manual advance behind \
              this one lock; the guard is never held across an await, and wakers are invoked only \
              after the guard is dropped"
)]
type SleeperLock = std::sync::Mutex<ManualClockState>;

/// A test clock: `advance` moves time and wakes sleepers whose deadline passed.
#[derive(Debug)]
pub struct ManualClock {
    state: SleeperLock,
}

#[derive(Debug)]
struct ManualClockState {
    now: Timestamp,
    next_sleeper_id: u64,
    sleepers: Vec<Sleeper>,
}

#[derive(Debug)]
struct Sleeper {
    id: u64,
    deadline: Timestamp,
    waker: Option<Waker>,
}

impl ManualClock {
    #[must_use]
    pub const fn new(start: Timestamp) -> Self {
        Self {
            state: SleeperLock::new(ManualClockState {
                now: start,
                next_sleeper_id: 0,
                sleepers: Vec::new(),
            }),
        }
    }

    /// Moves time forward and wakes every sleeper whose deadline was reached.
    ///
    /// # Panics
    ///
    /// Panics when the clock lock is poisoned.
    pub fn advance(&self, duration: Duration) {
        let due_wakers = {
            let mut state = self.state.lock().expect("manual clock lock poisoned");
            state.now = state.now.saturating_add(duration);
            let advanced_now = state.now;
            let mut due_wakers = Vec::new();
            state.sleepers.retain_mut(|sleeper| {
                if sleeper.deadline <= advanced_now {
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
}

impl Clock for ManualClock {
    fn now(&self) -> Timestamp {
        self.state.lock().expect("manual clock lock poisoned").now
    }

    fn sleep(&self, duration: Duration) -> Pin<Box<dyn Future<Output = ()> + Send + '_>> {
        let mut state = self.state.lock().expect("manual clock lock poisoned");
        let deadline = state.now.saturating_add(duration);
        let sleeper_id = state.next_sleeper_id;
        state.next_sleeper_id = sleeper_id.wrapping_add(1);
        state.sleepers.push(Sleeper {
            id: sleeper_id,
            deadline,
            waker: None,
        });
        drop(state);
        Box::pin(ManualSleep {
            state: &self.state,
            sleeper_id,
            deadline,
        })
    }
}

// A future is a short-lived view of the clock it sleeps on.
// ast-grep-ignore: types-own-their-data
struct ManualSleep<'clock> {
    state: &'clock SleeperLock,
    sleeper_id: u64,
    deadline: Timestamp,
}

impl Future for ManualSleep<'_> {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        let unpinned = self.get_mut();
        let mut state = unpinned.state.lock().expect("manual clock lock poisoned");
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
                .expect("a pending sleeper stays registered until its deadline passes");
            sleeper.waker = Some(cx.waker().clone());
            drop(state);
            Poll::Pending
        }
    }
}

impl Drop for ManualSleep<'_> {
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
    use std::sync::Arc;
    use std::task::Wake;

    use super::*;

    #[expect(
        clippy::disallowed_types,
        reason = "test-only wake flag; the woken test thread is the single writer"
    )]
    type WokenFlag = std::sync::Mutex<bool>;

    struct RecordingWake {
        woken: WokenFlag,
    }

    impl RecordingWake {
        fn new() -> Arc<Self> {
            Arc::new(Self {
                woken: WokenFlag::new(false),
            })
        }

        fn was_woken(&self) -> bool {
            *self.woken.lock().expect("wake flag lock poisoned")
        }
    }

    impl Wake for RecordingWake {
        fn wake(self: Arc<Self>) {
            *self.woken.lock().expect("wake flag lock poisoned") = true;
        }
    }

    #[test]
    fn manual_sleep_completes_only_when_advance_reaches_the_deadline() {
        let clock = ManualClock::new(Timestamp::UNIX_EPOCH);
        let mut sleep = clock.sleep(Duration::from_nanos(100));
        let recorder = RecordingWake::new();
        let waker = Waker::from(Arc::clone(&recorder));
        let mut context = Context::from_waker(&waker);

        assert!(
            sleep.as_mut().poll(&mut context).is_pending(),
            "a sleep must be pending before its deadline"
        );

        clock.advance(Duration::from_nanos(99));
        assert!(
            !recorder.was_woken(),
            "an advance short of the deadline must not wake the sleeper"
        );
        assert!(
            sleep.as_mut().poll(&mut context).is_pending(),
            "a sleep must stay pending short of its deadline"
        );

        clock.advance(Duration::from_nanos(1));
        assert!(
            recorder.was_woken(),
            "an advance onto the deadline must wake the sleeper"
        );
        assert!(
            sleep.as_mut().poll(&mut context).is_ready(),
            "a sleep must complete once its deadline is reached"
        );
    }

    #[test]
    fn manual_sleep_with_zero_duration_is_immediately_ready() {
        let clock = ManualClock::new(Timestamp::UNIX_EPOCH);
        let mut sleep = clock.sleep(Duration::ZERO);
        let recorder = RecordingWake::new();
        let waker = Waker::from(Arc::clone(&recorder));
        let mut context = Context::from_waker(&waker);

        assert!(
            sleep.as_mut().poll(&mut context).is_ready(),
            "a zero-duration sleep must complete on the first poll"
        );
    }

    #[test]
    fn manual_advance_moves_the_reported_time() {
        let clock = ManualClock::new(Timestamp::from_unix_nanos(10));
        clock.advance(Duration::from_nanos(5));
        assert_eq!(
            clock.now(),
            Timestamp::from_unix_nanos(15),
            "advance must move the reported time by exactly the given duration"
        );
    }

    #[test]
    fn timestamp_add_saturates_at_the_upper_bound() {
        let upper_bound = Timestamp::from_unix_nanos(u128::MAX);
        assert_eq!(
            upper_bound.saturating_add(Duration::from_nanos(1)),
            upper_bound,
            "adding past the representable range must saturate, not wrap"
        );
    }
}
