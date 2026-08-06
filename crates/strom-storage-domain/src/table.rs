//! Table object identity vocabulary.

use crate::SealGeneration;

#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, rkyv::Archive, rkyv::Serialize,
)]
pub struct TableObjectId {
    fresh: FreshIdentity,
    store: StoreKind,
}

impl TableObjectId {
    #[must_use]
    pub const fn new(fresh: FreshIdentity, store: StoreKind) -> Self {
        Self { fresh, store }
    }

    #[must_use]
    pub const fn fresh(self) -> FreshIdentity {
        self.fresh
    }

    #[must_use]
    pub const fn store(self) -> StoreKind {
        self.store
    }
}

impl TryFrom<&ArchivedTableObjectId> for TableObjectId {
    type Error = crate::archive::DecodeError;

    fn try_from(object: &ArchivedTableObjectId) -> Result<Self, Self::Error> {
        Ok(Self::new(
            FreshIdentity::try_from(&object.fresh)?,
            StoreKind::from(&object.store),
        ))
    }
}

#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, rkyv::Archive, rkyv::Serialize,
)]
pub struct FreshIdentity {
    birth_generation: SealGeneration,
    attempt: AttemptId,
    ordinal: u32,
}

impl FreshIdentity {
    /// # Errors
    ///
    /// Returns [`TableIdentityError`] unless the owner claim predates the birth generation.
    pub fn new(
        birth_generation: SealGeneration,
        attempt: AttemptId,
        ordinal: u32,
    ) -> Result<Self, TableIdentityError> {
        if attempt.owner_claim() >= birth_generation {
            return Err(TableIdentityError);
        }
        Ok(Self {
            birth_generation,
            attempt,
            ordinal,
        })
    }

    #[must_use]
    pub const fn birth_generation(self) -> SealGeneration {
        self.birth_generation
    }

    #[must_use]
    pub const fn attempt(self) -> AttemptId {
        self.attempt
    }

    #[must_use]
    pub const fn ordinal(self) -> u32 {
        self.ordinal
    }
}

impl TryFrom<&ArchivedFreshIdentity> for FreshIdentity {
    type Error = crate::archive::DecodeError;

    fn try_from(fresh: &ArchivedFreshIdentity) -> Result<Self, Self::Error> {
        Self::new(
            SealGeneration::from(&fresh.birth_generation),
            AttemptId::from(&fresh.attempt),
            fresh.ordinal.to_native(),
        )
        .map_err(|_domain_error| crate::archive::DecodeError::InvalidBody)
    }
}

#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, rkyv::Archive, rkyv::Serialize,
)]
pub struct AttemptId {
    owner_claim: SealGeneration,
    local_counter: u64,
}

impl AttemptId {
    #[must_use]
    pub const fn new(owner_claim: SealGeneration, local_counter: u64) -> Self {
        Self {
            owner_claim,
            local_counter,
        }
    }

    #[must_use]
    pub const fn owner_claim(self) -> SealGeneration {
        self.owner_claim
    }

    #[must_use]
    pub const fn local_counter(self) -> u64 {
        self.local_counter
    }
}

impl From<&ArchivedAttemptId> for AttemptId {
    fn from(attempt: &ArchivedAttemptId) -> Self {
        Self::new(
            SealGeneration::from(&attempt.owner_claim),
            attempt.local_counter.to_native(),
        )
    }
}

#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, rkyv::Archive, rkyv::Serialize,
)]
pub enum StoreKind {
    Directory,
    Ledger,
    Tally,
    Annals,
}

impl From<&ArchivedStoreKind> for StoreKind {
    fn from(store: &ArchivedStoreKind) -> Self {
        match store {
            ArchivedStoreKind::Directory => Self::Directory,
            ArchivedStoreKind::Ledger => Self::Ledger,
            ArchivedStoreKind::Tally => Self::Tally,
            ArchivedStoreKind::Annals => Self::Annals,
        }
    }
}

/// A table owner claim must predate the table's birth generation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("table owner claim does not predate its birth generation")]
pub struct TableIdentityError;
