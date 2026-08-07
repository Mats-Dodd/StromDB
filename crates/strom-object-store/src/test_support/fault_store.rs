#![expect(
    clippy::disallowed_types,
    reason = "the test store owns one enumerated state lock and never holds it across await"
)]

use std::fmt;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};

use async_trait::async_trait;
use bytes::Bytes;
use futures::stream::{self, BoxStream};
use futures::{StreamExt as _, TryStreamExt as _};
use object_store::memory::InMemory;
use object_store::path::Path;
use object_store::{
    CopyOptions, GetOptions, GetResult, GetResultPayload, ListResult, MultipartUpload, ObjectMeta,
    ObjectStore, ObjectStoreExt as _, PutMode, PutMultipartOptions, PutOptions, PutPayload,
    PutResult, Result as ObjectStoreResult,
};

use super::Gate;
use crate::ObjectKey;

/// An object-store operation that tests may select.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum Operation {
    Create,
    Read,
    List,
    Delete,
}

/// An exact coordinate or a segment-bounded family of coordinates.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub enum Target {
    Key(ObjectKey),
    Prefix(ObjectKey),
}

impl Target {
    const fn key(&self) -> &ObjectKey {
        match self {
            Self::Key(key) | Self::Prefix(key) => key,
        }
    }

    fn matches(&self, observed: &ObjectKey) -> bool {
        match self {
            Self::Key(expected) => expected == observed,
            Self::Prefix(prefix) => key_has_prefix(observed, prefix),
        }
    }

    fn overlaps(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Key(left), Self::Key(right)) => left == right,
            (Self::Key(key), Self::Prefix(prefix)) | (Self::Prefix(prefix), Self::Key(key)) => {
                key_has_prefix(key, prefix)
            }
            (Self::Prefix(left), Self::Prefix(right)) => {
                key_has_prefix(left, right) || key_has_prefix(right, left)
            }
        }
    }
}

/// One operation and target selected by a fault or gate.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct Selection {
    operation: Operation,
    target: Target,
}

impl Selection {
    #[must_use]
    pub const fn create(target: Target) -> Self {
        Self {
            operation: Operation::Create,
            target,
        }
    }

    #[must_use]
    pub const fn read(target: Target) -> Self {
        Self {
            operation: Operation::Read,
            target,
        }
    }

    #[must_use]
    pub const fn list(prefix: ObjectKey) -> Self {
        Self {
            operation: Operation::List,
            target: Target::Prefix(prefix),
        }
    }

    #[must_use]
    pub const fn delete(target: Target) -> Self {
        Self {
            operation: Operation::Delete,
            target,
        }
    }

    const fn exact(operation: Operation, key: ObjectKey) -> Self {
        let target = match operation {
            Operation::Create | Operation::Read | Operation::Delete => Target::Key(key),
            Operation::List => Target::Prefix(key),
        };
        Self { operation, target }
    }

    fn matches(&self, observed: &Self) -> bool {
        self.operation == observed.operation && self.target.matches(observed.target.key())
    }

    fn overlaps(&self, other: &Self) -> bool {
        self.operation == other.operation && self.target.overlaps(&other.target)
    }
}

impl fmt::Display for Selection {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{:?} {}", self.operation, self.target.key())
    }
}

/// A backend failure class with adapter-visible meaning.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BackendFailure {
    Transport,
    PermissionDenied,
    Unauthenticated,
}

/// One valid, one-shot object-store fault.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Fault {
    FailBefore {
        selection: Selection,
        failure: BackendFailure,
    },
    CreateThenLoseResponse {
        target: Target,
    },
    DeleteThenLoseResponse {
        target: Target,
    },
    FailBody {
        target: Target,
        failure: BackendFailure,
    },
    UnderreportMetadata {
        target: Target,
    },
    ReturnOutOfOrder {
        prefix: ObjectKey,
    },
    ReturnForeignKey {
        prefix: ObjectKey,
    },
}

impl Fault {
    fn selection(&self) -> Selection {
        match self {
            Self::FailBefore { selection, .. } => selection.clone(),
            Self::CreateThenLoseResponse { target } => Selection::create(target.clone()),
            Self::DeleteThenLoseResponse { target } => Selection::delete(target.clone()),
            Self::FailBody { target, .. } | Self::UnderreportMetadata { target } => {
                Selection::read(target.clone())
            }
            Self::ReturnOutOfOrder { prefix } | Self::ReturnForeignKey { prefix } => {
                Selection::list(prefix.clone())
            }
        }
    }
}

/// Invalid fault-store configuration.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum FaultStoreConfigError {
    #[error("fault selections overlap: {first} and {second}")]
    AmbiguousFault { first: Selection, second: Selection },
    #[error("gate selections overlap: {first} and {second}")]
    AmbiguousGate { first: Selection, second: Selection },
    #[error("the fault store was shared before configuration completed")]
    AlreadyShared,
}

/// A failed call-count assertion or incomplete fault-store run.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("{detail}")]
pub struct FaultStoreVerificationError {
    detail: String,
}

/// A test-only object store with pass-through behavior and narrow one-shot faults.
#[derive(Clone)]
pub struct FaultStore {
    inner: Arc<InMemory>,
    state: Arc<Mutex<State>>,
}

impl FaultStore {
    #[must_use]
    pub fn new() -> Self {
        Self {
            inner: Arc::new(InMemory::new()),
            state: Arc::new(Mutex::new(State::default())),
        }
    }

    /// Add one fault before sharing the backend.
    ///
    /// # Errors
    ///
    /// Returns an error when another fault can select the same call or the
    /// store was already cloned or exposed as a backend.
    pub fn inject(mut self, fault: Fault) -> Result<Self, FaultStoreConfigError> {
        let state = Arc::get_mut(&mut self.state).ok_or(FaultStoreConfigError::AlreadyShared)?;
        let state = match state.get_mut() {
            Ok(state) => state,
            Err(poisoned) => poisoned.into_inner(),
        };
        let selection = fault.selection();
        if let Some(existing) = state
            .faults
            .iter()
            .find(|existing| existing.selection.overlaps(&selection))
        {
            return Err(FaultStoreConfigError::AmbiguousFault {
                first: existing.selection.clone(),
                second: selection,
            });
        }
        state.faults.push(ConfiguredFault {
            selection,
            fault,
            status: FaultStatus::Available,
        });
        Ok(self)
    }

    /// Add one operation gate before sharing the backend.
    ///
    /// # Errors
    ///
    /// Returns an error when another gate can select the same call or the
    /// store was already cloned or exposed as a backend.
    pub fn gate(mut self, selection: Selection, gate: Gate) -> Result<Self, FaultStoreConfigError> {
        let state = Arc::get_mut(&mut self.state).ok_or(FaultStoreConfigError::AlreadyShared)?;
        let state = match state.get_mut() {
            Ok(state) => state,
            Err(poisoned) => poisoned.into_inner(),
        };
        if let Some(existing) = state
            .gates
            .iter()
            .find(|existing| existing.selection.overlaps(&selection))
        {
            return Err(FaultStoreConfigError::AmbiguousGate {
                first: existing.selection.clone(),
                second: selection,
            });
        }
        state.gates.push(ConfiguredGate {
            selection,
            gate,
            observed: false,
        });
        Ok(self)
    }

    /// A trait-object backend suitable for [`crate::ObjectStoreAdapter::new`].
    #[must_use]
    pub fn backend(&self) -> Arc<dyn ObjectStore> {
        Arc::new(self.clone())
    }

    /// Assert that one exact operation reached the backend once.
    ///
    /// # Errors
    ///
    /// Returns a diagnostic with the relevant operation log on mismatch.
    pub fn assert_called_once(
        &self,
        operation: Operation,
        key: &ObjectKey,
    ) -> Result<(), FaultStoreVerificationError> {
        let selection = Selection::exact(operation, key.clone());
        let state = self.state();
        let count = state
            .calls
            .iter()
            .filter(|observed| **observed == selection)
            .count();
        if count == 1 {
            drop(state);
            return Ok(());
        }
        let error = verification_error(
            &format!("expected {selection} once, observed {count} calls"),
            &state,
        );
        drop(state);
        Err(error)
    }

    /// Verify that every configured fault ran and every gate was reached.
    ///
    /// # Errors
    ///
    /// Returns one combined diagnostic for unused, cancelled, or ineffective
    /// configuration.
    pub fn verify(&self) -> Result<(), FaultStoreVerificationError> {
        let state = self.state();
        let mut failures = Vec::new();
        for configured in &state.faults {
            match &configured.status {
                FaultStatus::Applied => {}
                FaultStatus::Available => failures.push(format!(
                    "unused fault {:?} for {}",
                    configured.fault, configured.selection
                )),
                FaultStatus::Pending { call_id } => failures.push(format!(
                    "fault {:?} for {} remains pending on call {call_id}",
                    configured.fault, configured.selection
                )),
                FaultStatus::Ineffective { detail } => failures.push(format!(
                    "ineffective fault {:?} for {}: {detail}",
                    configured.fault, configured.selection
                )),
            }
        }
        for configured in &state.gates {
            if !configured.observed {
                failures.push(format!("unobserved gate for {}", configured.selection));
            }
        }
        if failures.is_empty() {
            drop(state);
            Ok(())
        } else {
            let error = verification_error(&failures.join("\n"), &state);
            drop(state);
            Err(error)
        }
    }

    fn begin(&self, selection: Selection) -> Attempt {
        let mut state = self.state();
        let call_id = state.next_call_id;
        state.next_call_id = state
            .next_call_id
            .checked_add(1)
            .expect("a test cannot exhaust operation identifiers");
        state.calls.push(selection.clone());

        let fault_index = state
            .faults
            .iter_mut()
            .enumerate()
            .find_map(|(index, configured)| {
                (configured.status == FaultStatus::Available
                    && configured.selection.matches(&selection))
                .then(|| {
                    configured.status = FaultStatus::Pending { call_id };
                    index
                })
            });

        let gates: Vec<Gate> = state
            .gates
            .iter_mut()
            .filter_map(|configured| {
                configured.selection.matches(&selection).then(|| {
                    configured.observed = true;
                    configured.gate.clone()
                })
            })
            .collect();
        state
            .log
            .push(format!("call {call_id} arrived: {selection}"));
        drop(state);

        Attempt {
            state: Arc::clone(&self.state),
            call_id,
            selection,
            fault_index,
            gates,
            finished: false,
        }
    }

    fn state(&self) -> std::sync::MutexGuard<'_, State> {
        match self.state.lock() {
            Ok(state) => state,
            Err(poisoned) => poisoned.into_inner(),
        }
    }

    async fn execute_list(
        &self,
        prefix: Option<Path>,
        offset: Option<Path>,
    ) -> ObjectStoreResult<BoxStream<'static, ObjectStoreResult<ObjectMeta>>> {
        let observed_prefix = object_key_from_path(prefix.as_ref().unwrap_or(&Path::from("list")))?;
        let selection = Selection::list(observed_prefix.clone());
        let mut attempt = self.begin(selection);
        attempt.wait_at_gates().await;
        let fault = attempt.fault();
        if let Some(Fault::FailBefore { failure, .. }) = fault {
            attempt.applied("failed before list reached storage");
            return Err(backend_error(failure, &observed_prefix));
        }

        let listing = match offset {
            Some(offset) => self.inner.list_with_offset(prefix.as_ref(), &offset),
            None => self.inner.list(prefix.as_ref()),
        };
        apply_list_fault(listing, &observed_prefix, fault, attempt).await
    }

    async fn delete_one(&self, location: Path) -> ObjectStoreResult<Path> {
        let key = object_key_from_path(&location)?;
        let mut attempt = self.begin(Selection::delete(Target::Key(key.clone())));
        attempt.wait_at_gates().await;
        match attempt.fault() {
            Some(Fault::FailBefore { failure, .. }) => {
                attempt.applied("failed before delete reached storage");
                Err(backend_error(failure, &key))
            }
            Some(Fault::DeleteThenLoseResponse { .. }) => {
                match self.inner.delete(&location).await {
                    Ok(()) => {
                        attempt.applied("delete took effect before its response was lost");
                        Err(backend_error(BackendFailure::Transport, &key))
                    }
                    Err(source) => {
                        Err(attempt.ineffective(format!("delete did not take effect: {source}")))
                    }
                }
            }
            None => {
                self.inner.delete(&location).await?;
                attempt.completed("delete passed through");
                Ok(location)
            }
            Some(_) => Err(attempt.ineffective("configured effect cannot apply to a delete")),
        }
    }
}

impl Default for FaultStore {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Debug for FaultStore {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("FaultStore")
    }
}

impl fmt::Display for FaultStore {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("StromDB fault store")
    }
}

#[async_trait]
impl ObjectStore for FaultStore {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> ObjectStoreResult<PutResult> {
        if opts.mode != PutMode::Create {
            return self.inner.put_opts(location, payload, opts).await;
        }
        let key = object_key_from_path(location)?;
        let mut attempt = self.begin(Selection::create(Target::Key(key.clone())));
        attempt.wait_at_gates().await;
        match attempt.fault() {
            Some(Fault::FailBefore { failure, .. }) => {
                attempt.applied("failed before create reached storage");
                Err(backend_error(failure, &key))
            }
            Some(Fault::CreateThenLoseResponse { .. }) => {
                match self.inner.put_opts(location, payload, opts).await {
                    Ok(_) => {
                        attempt.applied("create took effect before its response was lost");
                        Err(backend_error(BackendFailure::Transport, &key))
                    }
                    Err(source) => {
                        Err(attempt.ineffective(format!("create did not take effect: {source}")))
                    }
                }
            }
            None => {
                let result = self.inner.put_opts(location, payload, opts).await;
                attempt.completed("create passed through");
                result
            }
            Some(_) => Err(attempt.ineffective("configured effect cannot apply to a create")),
        }
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOptions,
    ) -> ObjectStoreResult<Box<dyn MultipartUpload>> {
        self.inner.put_multipart_opts(location, opts).await
    }

    async fn get_opts(&self, location: &Path, options: GetOptions) -> ObjectStoreResult<GetResult> {
        let key = object_key_from_path(location)?;
        let mut attempt = self.begin(Selection::read(Target::Key(key.clone())));
        attempt.wait_at_gates().await;
        match attempt.fault() {
            Some(Fault::FailBefore { failure, .. }) => {
                attempt.applied("failed before read reached storage");
                Err(backend_error(failure, &key))
            }
            Some(Fault::FailBody { failure, .. }) => {
                let mut result = match self.inner.get_opts(location, options).await {
                    Ok(result) => result,
                    Err(source) => {
                        return Err(attempt.ineffective(format!(
                            "body failure requires a readable object: {source}"
                        )));
                    }
                };
                if result.meta.size == 0 {
                    return Err(attempt.ineffective("body failure requires a non-empty body"));
                }
                let body_error = backend_error(failure, &key);
                result.payload =
                    GetResultPayload::Stream(Box::pin(BodyFailureStream::new(body_error, attempt)));
                Ok(result)
            }
            Some(Fault::UnderreportMetadata { .. }) => {
                let mut result = match self.inner.get_opts(location, options).await {
                    Ok(result) => result,
                    Err(source) => {
                        return Err(attempt.ineffective(format!(
                            "metadata underreport requires a readable object: {source}"
                        )));
                    }
                };
                if result.meta.size == 0 {
                    return Err(
                        attempt.ineffective("metadata underreport requires a non-empty body")
                    );
                }
                result.meta.size = result
                    .meta
                    .size
                    .checked_sub(1)
                    .expect("a non-empty body has positive metadata size");
                attempt.applied("underreported object size by one byte");
                Ok(result)
            }
            None => {
                let result = self.inner.get_opts(location, options).await;
                attempt.completed("read passed through");
                result
            }
            Some(_) => Err(attempt.ineffective("configured effect cannot apply to a read")),
        }
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, ObjectStoreResult<Path>>,
    ) -> BoxStream<'static, ObjectStoreResult<Path>> {
        let store = self.clone();
        locations
            .then(move |location| {
                let store = store.clone();
                async move {
                    let location = location?;
                    store.delete_one(location).await
                }
            })
            .boxed()
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, ObjectStoreResult<ObjectMeta>> {
        let store = self.clone();
        let prefix = prefix.cloned();
        stream::once(async move { store.execute_list(prefix, None).await })
            .try_flatten()
            .boxed()
    }

    fn list_with_offset(
        &self,
        prefix: Option<&Path>,
        offset: &Path,
    ) -> BoxStream<'static, ObjectStoreResult<ObjectMeta>> {
        let store = self.clone();
        let prefix = prefix.cloned();
        let offset = offset.clone();
        stream::once(async move { store.execute_list(prefix, Some(offset)).await })
            .try_flatten()
            .boxed()
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> ObjectStoreResult<ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy_opts(
        &self,
        from: &Path,
        to: &Path,
        options: CopyOptions,
    ) -> ObjectStoreResult<()> {
        self.inner.copy_opts(from, to, options).await
    }
}

async fn apply_list_fault(
    mut listing: BoxStream<'static, ObjectStoreResult<ObjectMeta>>,
    prefix: &ObjectKey,
    fault: Option<Fault>,
    mut attempt: Attempt,
) -> ObjectStoreResult<BoxStream<'static, ObjectStoreResult<ObjectMeta>>> {
    match fault {
        Some(Fault::ReturnOutOfOrder { .. }) => {
            let first =
                required_list_entry(&mut listing, &mut attempt, "out-of-order listing").await?;
            let second =
                required_list_entry(&mut listing, &mut attempt, "out-of-order listing").await?;
            attempt.applied("returned an out-of-order listing");
            Ok(stream::iter([Ok(second), Ok(first)]).chain(listing).boxed())
        }
        Some(Fault::ReturnForeignKey { .. }) => {
            let mut first =
                required_list_entry(&mut listing, &mut attempt, "foreign-key listing").await?;
            first.location = Path::from(format!("{prefix}/FOREIGN"));
            attempt.applied("returned a foreign key");
            Ok(stream::once(async move { Ok(first) })
                .chain(listing)
                .boxed())
        }
        None => {
            attempt.completed("list passed through");
            Ok(listing)
        }
        Some(
            Fault::CreateThenLoseResponse { .. }
            | Fault::DeleteThenLoseResponse { .. }
            | Fault::FailBody { .. }
            | Fault::UnderreportMetadata { .. },
        ) => Err(attempt.ineffective("configured effect cannot apply to a list")),
        Some(Fault::FailBefore { .. }) => {
            Err(attempt.ineffective("fail-before selection changed during list"))
        }
    }
}

async fn required_list_entry(
    listing: &mut BoxStream<'static, ObjectStoreResult<ObjectMeta>>,
    attempt: &mut Attempt,
    effect: &str,
) -> ObjectStoreResult<ObjectMeta> {
    match listing.next().await {
        Some(Ok(entry)) => Ok(entry),
        Some(Err(source)) => Err(attempt.ineffective(format!(
            "{effect} requires another readable entry: {source}"
        ))),
        None => Err(attempt.ineffective(format!("{effect} requires more stored entries"))),
    }
}

struct BodyFailureStream {
    error: Option<object_store::Error>,
    attempt: Option<Attempt>,
}

impl BodyFailureStream {
    const fn new(error: object_store::Error, attempt: Attempt) -> Self {
        Self {
            error: Some(error),
            attempt: Some(attempt),
        }
    }
}

impl futures::Stream for BodyFailureStream {
    type Item = ObjectStoreResult<Bytes>;

    fn poll_next(mut self: Pin<&mut Self>, _context: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let Some(error) = self.error.take() else {
            return Poll::Ready(None);
        };
        let mut attempt = self
            .attempt
            .take()
            .expect("an unconsumed body failure retains its attempt");
        attempt.applied("body emitted its injected streaming failure");
        Poll::Ready(Some(Err(error)))
    }
}

impl Drop for BodyFailureStream {
    fn drop(&mut self) {
        if let Some(mut attempt) = self.attempt.take() {
            attempt.mark_ineffective("body was dropped before its injected failure was emitted");
        }
    }
}

#[derive(Debug, Default)]
struct State {
    next_call_id: u64,
    faults: Vec<ConfiguredFault>,
    gates: Vec<ConfiguredGate>,
    calls: Vec<Selection>,
    log: Vec<String>,
}

#[derive(Debug)]
struct ConfiguredFault {
    selection: Selection,
    fault: Fault,
    status: FaultStatus,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum FaultStatus {
    Available,
    Pending { call_id: u64 },
    Applied,
    Ineffective { detail: String },
}

#[derive(Debug)]
struct ConfiguredGate {
    selection: Selection,
    gate: Gate,
    observed: bool,
}

struct Attempt {
    state: Arc<Mutex<State>>,
    call_id: u64,
    selection: Selection,
    fault_index: Option<usize>,
    gates: Vec<Gate>,
    finished: bool,
}

impl Attempt {
    async fn wait_at_gates(&self) {
        for gate in &self.gates {
            gate.block().await;
        }
    }

    fn fault(&self) -> Option<Fault> {
        let index = self.fault_index?;
        let state = self.state();
        state
            .faults
            .get(index)
            .map(|configured| configured.fault.clone())
    }

    fn applied(&mut self, detail: impl Into<String>) {
        let detail = detail.into();
        let mut state = self.state();
        if let Some(index) = self.fault_index {
            state
                .faults
                .get_mut(index)
                .expect("a reserved fault remains configured")
                .status = FaultStatus::Applied;
        }
        state.log.push(format!(
            "call {} applied: {} ({detail})",
            self.call_id, self.selection
        ));
        drop(state);
        self.finished = true;
    }

    fn completed(&mut self, detail: impl Into<String>) {
        let detail = detail.into();
        self.state().log.push(format!(
            "call {} completed: {} ({detail})",
            self.call_id, self.selection
        ));
        self.finished = true;
    }

    fn ineffective(&mut self, detail: impl Into<String>) -> object_store::Error {
        let detail = detail.into();
        self.mark_ineffective(detail.clone());
        mismatch_error(detail)
    }

    fn mark_ineffective(&mut self, detail: impl Into<String>) {
        let detail = detail.into();
        let mut state = self.state();
        if let Some(index) = self.fault_index {
            state
                .faults
                .get_mut(index)
                .expect("a reserved fault remains configured")
                .status = FaultStatus::Ineffective {
                detail: detail.clone(),
            };
        }
        state.log.push(format!(
            "call {} ineffective: {} ({detail})",
            self.call_id, self.selection
        ));
        drop(state);
        self.finished = true;
    }

    fn state(&self) -> std::sync::MutexGuard<'_, State> {
        match self.state.lock() {
            Ok(state) => state,
            Err(poisoned) => poisoned.into_inner(),
        }
    }
}

impl Drop for Attempt {
    fn drop(&mut self) {
        if self.finished {
            return;
        }
        let Ok(mut state) = self.state.lock() else {
            return;
        };
        if let Some(index) = self.fault_index
            && let Some(configured) = state.faults.get_mut(index)
        {
            configured.status = FaultStatus::Ineffective {
                detail: format!("selected call {} was cancelled", self.call_id),
            };
        }
        state.log.push(format!(
            "call {} cancelled: {}",
            self.call_id, self.selection
        ));
    }
}

fn key_has_prefix(key: &ObjectKey, prefix: &ObjectKey) -> bool {
    key == prefix
        || key
            .as_str()
            .strip_prefix(prefix.as_str())
            .is_some_and(|suffix| suffix.starts_with('/'))
}

fn object_key_from_path(path: &Path) -> ObjectStoreResult<ObjectKey> {
    ObjectKey::try_from(path.as_ref()).map_err(|source| object_store::Error::Generic {
        store: "strom fault store",
        source: Box::new(source),
    })
}

fn backend_error(failure: BackendFailure, key: &ObjectKey) -> object_store::Error {
    let path = key.to_string();
    let source = Box::new(InjectedBackendError { failure });
    match failure {
        BackendFailure::Transport => object_store::Error::Generic {
            store: "strom fault store",
            source,
        },
        BackendFailure::PermissionDenied => object_store::Error::PermissionDenied { path, source },
        BackendFailure::Unauthenticated => object_store::Error::Unauthenticated { path, source },
    }
}

fn mismatch_error(detail: impl Into<String>) -> object_store::Error {
    object_store::Error::Generic {
        store: "strom fault store",
        source: Box::new(FaultMismatch {
            detail: detail.into(),
        }),
    }
}

fn verification_error(detail: &str, state: &State) -> FaultStoreVerificationError {
    let operation_log = if state.log.is_empty() {
        "<empty>".to_owned()
    } else {
        state.log.join("\n")
    };
    FaultStoreVerificationError {
        detail: format!("{detail}\noperation log:\n{operation_log}"),
    }
}

#[derive(Debug, thiserror::Error)]
#[error("injected {failure:?} failure")]
struct InjectedBackendError {
    failure: BackendFailure,
}

#[derive(Debug, thiserror::Error)]
#[error("fault mismatch: {detail}")]
struct FaultMismatch {
    detail: String,
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use super::*;
    use crate::{ByteBound, CreateEvidence, FrozenBytes, ObjectStoreAdapter};

    #[test]
    fn overlapping_faults_fail_during_configuration() {
        let family = key("table/v1/ledger");
        let exact = key("table/v1/ledger/001");
        let store = FaultStore::new()
            .inject(Fault::CreateThenLoseResponse {
                target: Target::Prefix(family),
            })
            .expect("first fault is valid");

        let outcome = store.inject(Fault::FailBefore {
            selection: Selection::create(Target::Key(exact)),
            failure: BackendFailure::Transport,
        });

        assert!(
            matches!(outcome, Err(FaultStoreConfigError::AmbiguousFault { .. })),
            "overlapping selections must not depend on declaration order"
        );
    }

    fn key(raw: &str) -> ObjectKey {
        raw.parse().expect("test key is canonical")
    }

    fn body(raw: &[u8]) -> FrozenBytes {
        FrozenBytes::try_from(raw.to_vec()).expect("test body is legal")
    }

    fn read_bound() -> ByteBound {
        ByteBound::try_from(1024).expect("test bound is nonzero")
    }

    #[tokio::test]
    async fn unmatched_operations_delegate_to_memory() {
        let store = FaultStore::new();
        let adapter = ObjectStoreAdapter::new(store.backend());
        let coordinate = key("wal/v1/001");

        assert_eq!(
            CreateEvidence::Direct,
            adapter
                .create_if_absent(&coordinate, body(b"wal"))
                .await
                .expect("create passes through")
        );
        assert_eq!(
            b"wal",
            adapter
                .read(&coordinate, read_bound())
                .await
                .expect("read passes through")
                .expect("object exists")
                .body()
        );
        adapter
            .delete_idempotent(&coordinate)
            .await
            .expect("delete passes through");
        assert!(
            adapter
                .read(&coordinate, read_bound())
                .await
                .expect("read passes through")
                .is_none(),
            "delete removes the object"
        );
        store.verify().expect("empty configuration verifies");
    }

    #[tokio::test]
    async fn body_failure_is_ineffective_when_its_stream_is_dropped_unread() {
        let coordinate = key("wal/v1/body-drop");
        let store = FaultStore::new()
            .inject(Fault::FailBody {
                target: Target::Key(coordinate.clone()),
                failure: BackendFailure::Transport,
            })
            .expect("fault selection is unique");
        let backend = store.backend();
        let location = Path::from(coordinate.as_str());
        backend
            .put(&location, PutPayload::from_static(b"body"))
            .await
            .expect("test body stores without a create fault");

        let observation = backend
            .get(&location)
            .await
            .expect("metadata read succeeds");
        let pending = store
            .verify()
            .expect_err("the body error has not yet been emitted");
        assert!(
            pending.to_string().contains("remains pending"),
            "returning metadata does not apply the body fault: {pending}"
        );
        drop(observation);

        let diagnostic = store
            .verify()
            .expect_err("dropping the unread body makes the fault ineffective");
        assert!(
            diagnostic
                .to_string()
                .contains("body was dropped before its injected failure was emitted"),
            "verification names the unobserved body error: {diagnostic}"
        );
    }

    #[tokio::test]
    async fn unavailable_read_fixtures_are_ineffective_not_cancelled() {
        for (case, coordinate, fault) in [
            (
                "body failure",
                key("wal/v1/missing-body"),
                Fault::FailBody {
                    target: Target::Key(key("wal/v1/missing-body")),
                    failure: BackendFailure::Transport,
                },
            ),
            (
                "metadata underreport",
                key("wal/v1/missing-metadata"),
                Fault::UnderreportMetadata {
                    target: Target::Key(key("wal/v1/missing-metadata")),
                },
            ),
        ] {
            let store = FaultStore::new()
                .inject(fault)
                .expect("fault selection is unique");
            let backend = store.backend();

            backend
                .get(&Path::from(coordinate.as_str()))
                .await
                .expect_err("the required read fixture is absent");
            let diagnostic = store
                .verify()
                .expect_err("the configured fault could not run");
            let detail = diagnostic.to_string();
            assert!(
                detail.contains("requires a readable object") && !detail.contains("cancelled"),
                "{case} reports its unavailable fixture: {detail}"
            );
        }
    }

    #[tokio::test]
    async fn list_transforms_poll_only_the_entries_their_effect_requires() {
        let prefix = key("table/v1/ledger");
        let memory = InMemory::new();
        for suffix in ["001", "002", "003"] {
            memory
                .put(
                    &Path::from(format!("{prefix}/{suffix}")),
                    PutPayload::from_static(b"table"),
                )
                .await
                .expect("test list entry stores");
        }
        let entries = memory
            .list(Some(&Path::from(prefix.as_str())))
            .try_collect::<Vec<_>>()
            .await
            .expect("test entries list");

        let pass_through_polls = Arc::new(AtomicUsize::new(0));
        let observed_polls = Arc::clone(&pass_through_polls);
        let listing = stream::iter(entries.clone().into_iter().map(Ok))
            .inspect(move |_| {
                observed_polls.fetch_add(1, Ordering::SeqCst);
            })
            .boxed();
        let store = FaultStore::new();
        let attempt = store.begin(Selection::list(prefix.clone()));
        let mut transformed = apply_list_fault(listing, &prefix, None, attempt)
            .await
            .expect("pass-through list is built");
        assert_eq!(
            0,
            pass_through_polls.load(Ordering::SeqCst),
            "building a pass-through listing does not poll the backend"
        );
        transformed
            .next()
            .await
            .expect("one entry exists")
            .expect("entry is readable");
        assert_eq!(
            1,
            pass_through_polls.load(Ordering::SeqCst),
            "one consumer poll reaches exactly one backend entry"
        );

        for (case, fault, entries_buffered) in [
            (
                "out of order",
                Fault::ReturnOutOfOrder {
                    prefix: prefix.clone(),
                },
                2,
            ),
            (
                "foreign key",
                Fault::ReturnForeignKey {
                    prefix: prefix.clone(),
                },
                1,
            ),
        ] {
            let polls = Arc::new(AtomicUsize::new(0));
            let observed_polls = Arc::clone(&polls);
            let listing = stream::iter(entries.clone().into_iter().map(Ok))
                .inspect(move |_| {
                    observed_polls.fetch_add(1, Ordering::SeqCst);
                })
                .boxed();
            let store = FaultStore::new()
                .inject(fault.clone())
                .expect("fault selection is unique");
            let attempt = store.begin(Selection::list(prefix.clone()));
            let mut transformed = apply_list_fault(listing, &prefix, Some(fault), attempt)
                .await
                .expect("list fault is built");
            assert_eq!(
                entries_buffered,
                polls.load(Ordering::SeqCst),
                "{case} buffers only the entries needed to make the fault real"
            );
            transformed
                .next()
                .await
                .expect("one transformed entry exists")
                .expect("transformed entry is readable");
            assert_eq!(
                entries_buffered,
                polls.load(Ordering::SeqCst),
                "serving a buffered {case} entry does not pull the remainder"
            );
            store.verify().expect("the list fault ran");
        }
    }

    #[tokio::test]
    async fn faults_are_one_shot_and_verified() {
        let coordinate = key("wal/v1/002");
        let store = FaultStore::new()
            .inject(Fault::FailBefore {
                selection: Selection::create(Target::Key(coordinate.clone())),
                failure: BackendFailure::Transport,
            })
            .expect("fault selection is unique");
        let adapter = ObjectStoreAdapter::new(store.backend());

        assert_eq!(
            CreateEvidence::Unresolved,
            adapter
                .create_if_absent(&coordinate, body(b"wal"))
                .await
                .expect("ambiguous create is evidence")
        );
        assert_eq!(
            CreateEvidence::Direct,
            adapter
                .create_if_absent(&coordinate, body(b"wal"))
                .await
                .expect("the second call passes through")
        );
        store.verify().expect("the one-shot fault ran");
    }

    #[tokio::test]
    async fn gate_reports_arrival_before_release() {
        let coordinate = key("seal/v1/001");
        let gate = Gate::new();
        let store = FaultStore::new()
            .gate(
                Selection::create(Target::Key(coordinate.clone())),
                gate.clone(),
            )
            .expect("gate selection is unique");
        let adapter = ObjectStoreAdapter::new(store.backend());
        let operation =
            tokio::spawn(async move { adapter.create_if_absent(&coordinate, body(b"seal")).await });

        gate.wait_until_blocked().await;
        assert!(
            !operation.is_finished(),
            "the selected operation remains blocked"
        );
        gate.release();
        assert_eq!(
            CreateEvidence::Direct,
            operation
                .await
                .expect("operation task completes")
                .expect("create passes after release")
        );
        store.verify().expect("the gate observed its call");
    }

    #[tokio::test]
    async fn impossible_after_effect_is_reported_with_the_operation_log() {
        let coordinate = key("wal/v1/003");
        let store = FaultStore::new()
            .inject(Fault::CreateThenLoseResponse {
                target: Target::Key(coordinate.clone()),
            })
            .expect("fault selection is unique");
        let backend = store.backend();
        backend
            .put(
                &Path::from(coordinate.as_str()),
                PutPayload::from_static(b"occupant"),
            )
            .await
            .expect("test occupant stores without a create fault");
        let adapter = ObjectStoreAdapter::new(backend);

        let outcome = adapter
            .create_if_absent(&coordinate, body(b"candidate"))
            .await;
        assert_eq!(
            Ok(CreateEvidence::Unresolved),
            outcome,
            "a fault mismatch is still ambiguous to the production adapter"
        );
        let diagnostic = store.verify().expect_err("after-effect did not occur");
        let detail = diagnostic.to_string();
        assert!(
            detail.contains("create did not take effect") && detail.contains("operation log"),
            "verification explains the ineffective fault: {detail}"
        );
    }

    #[tokio::test]
    async fn cancellation_prevents_a_reserved_fault_from_verifying() {
        let coordinate = key("wal/v1/004");
        let gate = Gate::new();
        let selection = Selection::create(Target::Key(coordinate.clone()));
        let store = FaultStore::new()
            .inject(Fault::FailBefore {
                selection: selection.clone(),
                failure: BackendFailure::Transport,
            })
            .expect("fault selection is unique")
            .gate(selection, gate.clone())
            .expect("gate selection is unique");
        let adapter = ObjectStoreAdapter::new(store.backend());
        let operation = tokio::spawn(async move {
            adapter
                .create_if_absent(&coordinate, body(b"candidate"))
                .await
        });

        gate.wait_until_blocked().await;
        operation.abort();
        assert!(
            operation
                .await
                .expect_err("operation is cancelled")
                .is_cancelled(),
            "the task stopped at the gate"
        );
        let diagnostic = store.verify().expect_err("reserved fault never ran");
        assert!(
            diagnostic.to_string().contains("cancelled"),
            "verification names the cancellation: {diagnostic}"
        );
    }
}
