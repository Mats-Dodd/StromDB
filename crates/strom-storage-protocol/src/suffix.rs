//! Shared WAL-suffix coordinate semantics.

use strom_storage_domain::{BatchId, WAL_SUFFIX_COORDINATES_MAX_V2};

/// Why a takeover FENCE cannot bound a recoverable WAL suffix.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum TakeoverFenceError {
    NoRunCoordinate,
    NotAfterCut,
    SpanExceeded { span: u64 },
}

/// A takeover FENCE within the suffix bound with a later RUN coordinate.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct TakeoverFence {
    batch: BatchId,
}

impl TakeoverFence {
    #[must_use]
    pub(crate) const fn batch(self) -> BatchId {
        self.batch
    }
}

/// Decide whether `run` fits while reserving its successor for takeover.
#[must_use]
pub(crate) fn run_leaves_takeover_room(cut: Option<BatchId>, run: BatchId) -> bool {
    run.successor()
        .ok()
        .and_then(|fence| span_through(cut, fence))
        .is_some_and(|span| span > 1 && span <= WAL_SUFFIX_COORDINATES_MAX_V2)
}

/// Prove that `fence` bounds the suffix and leaves a RUN coordinate after it.
pub(crate) fn bound_takeover_fence(
    cut: Option<BatchId>,
    fence: BatchId,
) -> Result<TakeoverFence, TakeoverFenceError> {
    fence
        .successor()
        .map_err(|_exhausted| TakeoverFenceError::NoRunCoordinate)?;
    let span = span_through(cut, fence)
        .filter(|span| *span > 0)
        .ok_or(TakeoverFenceError::NotAfterCut)?;
    if span > WAL_SUFFIX_COORDINATES_MAX_V2 {
        return Err(TakeoverFenceError::SpanExceeded { span });
    }
    Ok(TakeoverFence { batch: fence })
}

/// Return the inclusive coordinate span after `cut` through `head`.
#[must_use]
pub(crate) const fn span_through(cut: Option<BatchId>, head: BatchId) -> Option<u64> {
    let cut = match cut {
        None => 0,
        Some(batch) => batch.get(),
    };
    head.get().checked_sub(cut)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn run_room_reserves_one_takeover_coordinate() -> Result<(), Box<dyn std::error::Error>> {
        let last_genesis_run = BatchId::try_from(WAL_SUFFIX_COORDINATES_MAX_V2 - 1)?;
        assert!(run_leaves_takeover_room(None, last_genesis_run));
        assert!(!run_leaves_takeover_room(
            None,
            last_genesis_run.successor()?
        ));

        let cut = BatchId::try_from(u64::MAX - 2)?;
        assert!(run_leaves_takeover_room(
            Some(cut),
            BatchId::try_from(u64::MAX - 1)?
        ));
        assert!(!run_leaves_takeover_room(
            Some(cut),
            BatchId::try_from(u64::MAX)?
        ));
        Ok(())
    }

    #[test]
    fn takeover_fence_requires_a_bounded_span_and_later_run()
    -> Result<(), Box<dyn std::error::Error>> {
        let cut = BatchId::try_from(50)?;
        let at_limit = BatchId::try_from(cut.get() + WAL_SUFFIX_COORDINATES_MAX_V2)?;
        let Ok(bounded) = bound_takeover_fence(Some(cut), at_limit) else {
            return Err("the fixture fence must fit the exact suffix bound".into());
        };
        assert_eq!(at_limit, bounded.batch());

        let over_limit = at_limit.successor()?;
        assert_eq!(
            Err(TakeoverFenceError::SpanExceeded {
                span: WAL_SUFFIX_COORDINATES_MAX_V2 + 1,
            }),
            bound_takeover_fence(Some(cut), over_limit)
        );
        assert_eq!(
            Err(TakeoverFenceError::NotAfterCut),
            bound_takeover_fence(Some(cut), cut)
        );
        assert_eq!(
            Err(TakeoverFenceError::NoRunCoordinate),
            bound_takeover_fence(Some(cut), BatchId::try_from(u64::MAX)?)
        );
        Ok(())
    }
}
