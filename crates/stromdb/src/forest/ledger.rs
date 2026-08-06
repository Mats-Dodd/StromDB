//! Resident Ledger: stream records keyed by dense UID.

use imbl::OrdMap;
use strom_storage_domain::{StreamRecord, StreamUid};

/// In-memory Ledger values for strict fold.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct ResidentLedger {
    records: OrdMap<StreamUid, StreamRecord>,
}

impl ResidentLedger {
    #[must_use]
    pub(super) fn empty() -> Self {
        Self {
            records: OrdMap::new(),
        }
    }

    #[must_use]
    pub(super) const fn from_records(records: OrdMap<StreamUid, StreamRecord>) -> Self {
        Self { records }
    }

    #[must_use]
    pub(super) fn get(&self, uid: StreamUid) -> Option<&StreamRecord> {
        self.records.get(&uid)
    }

    pub(super) fn insert(&mut self, uid: StreamUid, record: StreamRecord) {
        let previous = self.records.insert(uid, record);
        assert!(previous.is_none(), "create folds a fresh ledger uid");
    }

    pub(super) fn replace(&mut self, uid: StreamUid, record: StreamRecord) {
        let previous = self.records.insert(uid, record);
        assert!(previous.is_some(), "close folds an existing ledger record");
    }

    pub(super) fn remove(&mut self, uid: StreamUid) {
        let previous = self.records.remove(&uid);
        assert!(
            previous.is_some(),
            "delete removes an existing ledger record"
        );
    }
}
