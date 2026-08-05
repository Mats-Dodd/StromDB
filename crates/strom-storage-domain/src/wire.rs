//! Private wire representations for protocol-domain values.

use serde::Deserialize;
use strom_domain::{ExpiresAt, ExpiryPolicy, StreamContentType, StreamLifecycle, StreamTtl};

use crate::envelope::DecodeError;

#[derive(Debug, Deserialize)]
#[cfg_attr(test, derive(serde::Serialize))]
pub(crate) enum ExpiryPolicyWire {
    None,
    SlidingTtl(u64),
    AbsoluteExpiry(i128),
}

impl TryFrom<ExpiryPolicyWire> for ExpiryPolicy {
    type Error = DecodeError;

    fn try_from(wire: ExpiryPolicyWire) -> Result<Self, Self::Error> {
        match wire {
            ExpiryPolicyWire::None => Ok(Self::None),
            ExpiryPolicyWire::SlidingTtl(seconds) => std::num::NonZeroU64::new(seconds)
                .map(StreamTtl::from)
                .map(Self::SlidingTtl)
                .ok_or(DecodeError::InvalidBody),
            ExpiryPolicyWire::AbsoluteExpiry(unix_nanoseconds) => {
                ExpiresAt::try_from(unix_nanoseconds)
                    .map(Self::AbsoluteExpiry)
                    .map_err(|_detail| DecodeError::InvalidBody)
            }
        }
    }
}

pub(crate) fn parse_content_type(raw: &str) -> Result<StreamContentType, DecodeError> {
    raw.parse().map_err(|_detail| DecodeError::InvalidBody)
}

#[derive(Debug, Deserialize)]
#[cfg_attr(test, derive(serde::Serialize))]
pub(crate) enum StreamLifecycleWire {
    Open,
    Closed,
}

impl From<StreamLifecycleWire> for StreamLifecycle {
    fn from(wire: StreamLifecycleWire) -> Self {
        match wire {
            StreamLifecycleWire::Open => Self::Open,
            StreamLifecycleWire::Closed => Self::Closed,
        }
    }
}
