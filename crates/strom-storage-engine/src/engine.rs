//! Engine startup, immutable views, bounded commands, and graceful drain.

use std::sync::Arc;
use std::time::Duration;

use object_store::ObjectStore;
use strom_common::{Entropy, MonotonicClock};
use strom_domain::{
    CloseStreamOutcome, CreateOutcome, ExpiryPolicy, StreamContentType, StreamLifecycle,
    StreamPath, StreamStatus,
};
use strom_object_store::ObjectStoreAdapter;
use strom_storage_domain::{
    DirectoryEntry, PartitionId, SealGeneration, WAL_ENCODED_BYTES_MAX, WRITER_INGRESS_COMMANDS_MAX,
};
use strom_storage_protocol::{
    AdmissionRefusal, BootstrapExit, CommandEnvelope, CreateStream, Forest, WriterExit,
};
use tokio::sync::{mpsc, oneshot, watch};
use tokio::task::JoinHandle;

use crate::bootstrap::bootstrap;
use crate::writer::spawn_writer;

const FLUSH_INTERVAL_DEFAULT: Duration = Duration::from_millis(250);

/// Validated WAL flush pacing policy.
///
/// By default, time-triggered WAL starts are spaced by 250 milliseconds and
/// the soft byte threshold is the 4 MiB hard WAL object bound.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Options {
    flush_interval_min: Duration,
    flush_buffer_bytes: usize,
}

impl Default for Options {
    fn default() -> Self {
        Self {
            flush_interval_min: FLUSH_INTERVAL_DEFAULT,
            flush_buffer_bytes: WAL_ENCODED_BYTES_MAX,
        }
    }
}

impl Options {
    /// Set the minimum spacing between time-triggered WAL starts.
    ///
    /// Zero restores eager flush-on-completion timing. Byte pressure, hard
    /// capacity, and shutdown may start a WAL flight before this interval.
    #[must_use]
    pub const fn with_min_flush_interval(mut self, duration: Duration) -> Self {
        self.flush_interval_min = duration;
        self
    }

    /// Set the soft estimated-byte threshold that starts a WAL flush.
    ///
    /// # Errors
    ///
    /// Returns [`OptionsError`] when `bytes` is zero or exceeds the hard WAL
    /// object bound.
    pub const fn with_flush_buffer_bytes(mut self, bytes: usize) -> Result<Self, OptionsError> {
        if bytes == 0 {
            return Err(OptionsError::FlushBufferBytesZero);
        }
        if bytes > WAL_ENCODED_BYTES_MAX {
            return Err(OptionsError::FlushBufferBytesOverMax { bytes });
        }
        self.flush_buffer_bytes = bytes;
        Ok(self)
    }

    pub(crate) const fn flush_interval_min(self) -> Duration {
        self.flush_interval_min
    }

    pub(crate) const fn flush_buffer_bytes(self) -> usize {
        self.flush_buffer_bytes
    }
}

/// Why a writer option could not be constructed.
#[derive(Clone, Copy, Debug, PartialEq, Eq, thiserror::Error)]
pub enum OptionsError {
    #[error("flush buffer byte budget must be nonzero")]
    FlushBufferBytesZero,
    #[error("flush buffer byte budget {bytes} exceeds the {WAL_ENCODED_BYTES_MAX}-byte WAL bound")]
    FlushBufferBytesOverMax { bytes: usize },
}

#[derive(Debug, Clone)]
pub(crate) struct PublishedView {
    forest: Forest,
}

impl PublishedView {
    pub(crate) const fn new(forest: Forest) -> Self {
        Self { forest }
    }

    fn stream(&self, path: &StreamPath) -> StreamStatus {
        match self.forest.resolve(path) {
            None => StreamStatus::Missing,
            Some(DirectoryEntry::Tombstone(_uid)) => StreamStatus::Deleted,
            Some(DirectoryEntry::Live(uid)) => {
                let record = self
                    .forest
                    .record(uid)
                    .expect("a live directory entry has a ledger record");
                StreamStatus::Live {
                    content_type: record.content_type().clone(),
                    expiry: record.expiry(),
                    lifecycle: record.lifecycle(),
                }
            }
        }
    }
}

#[derive(Debug)]
pub struct Engine {
    partition: PartitionId,
    commands: mpsc::Sender<CommandEnvelope>,
    view: watch::Receiver<PublishedView>,
    writer: JoinHandle<WriterExit>,
}

impl Engine {
    /// Bootstrap one partition and start its sole writer.
    ///
    /// # Errors
    ///
    /// Returns [`OpenError`] unless the complete durable state is bounded,
    /// consistent, directly claimed, fenced, replayed, and current.
    pub async fn open(
        store: Arc<dyn ObjectStore>,
        entropy: Entropy,
        clock: Arc<dyn MonotonicClock>,
        options: Options,
    ) -> Result<Self, OpenError> {
        let adapter = ObjectStoreAdapter::new(store);
        let recovery = bootstrap(adapter.clone(), entropy)
            .await
            .map_err(open_error)?;
        let partition = recovery.partition();
        let initial = PublishedView::new(recovery.durable_forest().clone());
        let (view_sender, view) = watch::channel(initial);
        let (commands, ingress) = mpsc::channel(WRITER_INGRESS_COMMANDS_MAX);
        let writer = spawn_writer(adapter, recovery, ingress, view_sender, clock, options);
        Ok(Self {
            partition,
            commands,
            view,
            writer,
        })
    }

    #[must_use]
    pub const fn partition_id(&self) -> PartitionId {
        self.partition
    }

    /// Create one stream or confirm its durable configuration.
    ///
    /// # Errors
    ///
    /// Returns [`StreamError`] when the command is refused, shed, unavailable,
    /// or left without a determinate durable outcome.
    pub async fn create_stream(
        &self,
        path: &StreamPath,
        content_type: StreamContentType,
        expiry: ExpiryPolicy,
        lifecycle: StreamLifecycle,
    ) -> Result<CreateOutcome, StreamError> {
        let (reply, outcome) = oneshot::channel();
        let command = CreateStream {
            path: path.clone(),
            content_type,
            expiry,
            lifecycle,
        };
        enqueue(&self.commands, CommandEnvelope::Create { command, reply }).await?;
        match outcome.await {
            Ok(Ok(reply)) => Ok(reply),
            Ok(Err(refusal)) => Err(stream_error(refusal)),
            Err(_writer_dropped_waiter) => Err(StreamError::Indeterminate),
        }
    }

    /// Close one live stream or confirm that it is already closed.
    ///
    /// # Errors
    ///
    /// Returns [`StreamError`] when the command is refused, shed, unavailable,
    /// or left without a determinate durable outcome.
    pub async fn close_stream(&self, path: &StreamPath) -> Result<CloseStreamOutcome, StreamError> {
        let (reply, outcome) = oneshot::channel();
        enqueue(
            &self.commands,
            CommandEnvelope::Close {
                path: path.clone(),
                reply,
            },
        )
        .await?;
        match outcome.await {
            Ok(Ok(reply)) => Ok(reply),
            Ok(Err(refusal)) => Err(stream_error(refusal)),
            Err(_writer_dropped_waiter) => Err(StreamError::Indeterminate),
        }
    }

    /// Delete one live stream.
    ///
    /// # Errors
    ///
    /// Returns [`StreamError`] when the command is refused, shed, unavailable,
    /// or left without a determinate durable outcome.
    pub async fn delete_stream(&self, path: &StreamPath) -> Result<(), StreamError> {
        let (reply, outcome) = oneshot::channel();
        enqueue(
            &self.commands,
            CommandEnvelope::Delete {
                path: path.clone(),
                reply,
            },
        )
        .await?;
        match outcome.await {
            Ok(Ok(())) => Ok(()),
            Ok(Err(refusal)) => Err(stream_error(refusal)),
            Err(_writer_dropped_waiter) => Err(StreamError::Indeterminate),
        }
    }

    /// Report the current protocol-visible state of one stream.
    ///
    /// # Errors
    ///
    /// Returns [`StreamError::Unavailable`] after the writer revokes readiness.
    pub fn stream(&self, path: &StreamPath) -> Result<StreamStatus, StreamError> {
        if self.commands.is_closed() {
            return Err(StreamError::Unavailable);
        }
        Ok(self.view.borrow().stream(path))
    }

    /// Close ingress, drain every accepted command, and join the writer.
    ///
    /// # Panics
    ///
    /// Panics when the writer task was externally cancelled or violated an
    /// in-process invariant.
    pub async fn shutdown(self) -> CloseOutcome {
        let Self {
            partition: _,
            commands,
            view,
            writer,
        } = self;
        drop(commands);
        let exit = writer
            .await
            .expect("the partition writer exits without cancellation or panic");
        drop(view);
        close_outcome(exit)
    }
}

async fn enqueue(
    commands: &mpsc::Sender<CommandEnvelope>,
    command: CommandEnvelope,
) -> Result<(), StreamError> {
    match commands.send(command).await {
        Ok(()) => Ok(()),
        Err(mpsc::error::SendError(_envelope)) => Err(StreamError::Unavailable),
    }
}

/// Why one partition could not open.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum OpenError {
    #[error("open should be retried: {detail}")]
    Retryable { detail: String },
    #[error("another writer took the partition at Seal generation {observed:?}")]
    Fenced { observed: SealGeneration },
    #[error("durable state contradicts the storage model: {detail}")]
    Contradiction { detail: String },
}

/// Why one stream operation did not take effect.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum StreamError {
    #[error("stream path is already occupied")]
    Occupied,
    #[error("partition stream capacity is exhausted")]
    CapacityExhausted,
    #[error("stream path is not live")]
    NotLive,
    #[error("partition is at a bounded capacity limit; retry later")]
    Overloaded,
    #[error("partition is no longer serving")]
    Unavailable,
    #[error("operation outcome is indeterminate; reopen and inspect")]
    Indeterminate,
}

/// How one partition stopped serving.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CloseOutcome {
    /// Every accepted command drained and the partition stopped cleanly.
    Shutdown,
    /// Another writer took the partition.
    Fenced,
    /// An operation may have taken effect without local evidence.
    Poisoned { detail: String },
    /// Durable state contradicts the storage model.
    Contradiction { detail: String },
}

fn open_error(exit: BootstrapExit) -> OpenError {
    match exit {
        BootstrapExit::Retryable { detail } => OpenError::Retryable { detail },
        BootstrapExit::Fenced { observed } => OpenError::Fenced { observed },
        BootstrapExit::Contradiction { detail } => OpenError::Contradiction { detail },
    }
}

const fn stream_error(refusal: AdmissionRefusal) -> StreamError {
    match refusal {
        AdmissionRefusal::PathOccupied => StreamError::Occupied,
        AdmissionRefusal::PathCapacityExhausted => StreamError::CapacityExhausted,
        AdmissionRefusal::PathNotLive => StreamError::NotLive,
        AdmissionRefusal::Overloaded => StreamError::Overloaded,
    }
}

fn close_outcome(exit: WriterExit) -> CloseOutcome {
    match exit {
        WriterExit::Shutdown => CloseOutcome::Shutdown,
        WriterExit::Fenced { batch: _ } => CloseOutcome::Fenced,
        WriterExit::Poisoned { batch: _, detail } => CloseOutcome::Poisoned { detail },
        WriterExit::Contradiction { batch: _, detail } => CloseOutcome::Contradiction { detail },
    }
}

#[cfg(test)]
mod tests {
    use futures::poll;

    use super::*;

    #[test]
    fn flush_byte_options_accept_the_closed_valid_interval() {
        assert!(
            Options::default().with_flush_buffer_bytes(1).is_ok(),
            "one byte is the smallest valid soft budget"
        );
        assert!(
            Options::default()
                .with_flush_buffer_bytes(WAL_ENCODED_BYTES_MAX)
                .is_ok(),
            "the hard WAL object bound is a valid soft budget"
        );
        assert_eq!(
            Err(OptionsError::FlushBufferBytesZero),
            Options::default().with_flush_buffer_bytes(0)
        );
        assert_eq!(
            Err(OptionsError::FlushBufferBytesOverMax {
                bytes: WAL_ENCODED_BYTES_MAX + 1,
            }),
            Options::default().with_flush_buffer_bytes(WAL_ENCODED_BYTES_MAX + 1)
        );
    }

    #[tokio::test]
    async fn awaited_enqueue_is_cancellation_safe_on_both_sides_of_transfer()
    -> Result<(), Box<dyn std::error::Error>> {
        let (commands, mut ingress) = mpsc::channel(1);
        commands.send(delete_envelope("events/first")?).await?;

        let mut waiting = Box::pin(enqueue(&commands, delete_envelope("events/cancelled")?));
        assert!(
            poll!(waiting.as_mut()).is_pending(),
            "enqueue awaits capacity instead of refusing a full channel"
        );
        drop(waiting);
        drop(ingress.recv().await);
        assert!(
            matches!(ingress.try_recv(), Err(mpsc::error::TryRecvError::Empty)),
            "cancelling a capacity wait withdraws that send"
        );

        enqueue(&commands, delete_envelope("events/transferred")?).await?;
        drop(commands);
        assert!(
            ingress.recv().await.is_some(),
            "a completed enqueue transfers command ownership to ingress"
        );
        Ok(())
    }

    #[tokio::test]
    async fn channel_closure_while_enqueue_waits_reports_unavailable()
    -> Result<(), Box<dyn std::error::Error>> {
        let (commands, mut ingress) = mpsc::channel(1);
        commands.send(delete_envelope("events/first")?).await?;
        let mut waiting = Box::pin(enqueue(&commands, delete_envelope("events/waiting")?));
        assert!(poll!(waiting.as_mut()).is_pending());
        ingress.close();
        assert_eq!(Err(StreamError::Unavailable), waiting.await);
        Ok(())
    }

    fn delete_envelope(raw: &str) -> Result<CommandEnvelope, strom_domain::StreamPathError> {
        let (reply, _outcome) = oneshot::channel();
        Ok(CommandEnvelope::Delete {
            path: raw.parse()?,
            reply,
        })
    }
}
