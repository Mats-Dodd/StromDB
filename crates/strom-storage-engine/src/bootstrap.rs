//! Sequential interpreter for the pure bootstrap correctness machine.

use rand::RngCore as _;
use strom_common::Entropy;
use strom_object_store::ObjectStoreAdapter;
use strom_storage_domain::PartitionId;
use strom_storage_protocol::{
    BootstrapEffect, BootstrapEvent, BootstrapExit, BootstrapMachine, BootstrapStep,
    TypedStoreError, WriterRecovery,
};

use crate::store::{ObservedWal, SealStore, TableStore, WalStore};

pub(crate) async fn bootstrap(
    adapter: ObjectStoreAdapter,
    mut entropy: Entropy,
) -> Result<WriterRecovery, BootstrapExit> {
    let seal_store = SealStore::new(adapter.clone());
    let wal_store = WalStore::new(adapter.clone());
    let table_store = TableStore::new(adapter);
    let mut genesis_entropy = entropy.fork("partition-id");
    let mut machine = BootstrapMachine::new();
    let mut step = machine.handle(BootstrapEvent::Started {
        genesis_partition: mint_partition_id(&mut genesis_entropy),
    });

    loop {
        let event = match step {
            BootstrapStep::Effect(effect) => {
                execute_effect(&seal_store, &wal_store, &table_store, effect).await
            }
            BootstrapStep::Complete(recovery) => return Ok(recovery),
            BootstrapStep::Exit(exit) => return Err(exit),
        };
        step = machine.handle(event);
    }
}

async fn execute_effect(
    seal_store: &SealStore,
    wal_store: &WalStore,
    table_store: &TableStore,
    effect: BootstrapEffect,
) -> BootstrapEvent {
    match effect {
        BootstrapEffect::ObserveHead => complete(
            seal_store.newest_generation().await,
            BootstrapEvent::HeadObserved,
        ),
        BootstrapEffect::EstablishGenesis(candidate) => complete(
            seal_store.establish_genesis(&candidate).await,
            BootstrapEvent::GenesisEstablished,
        ),
        BootstrapEffect::ReadSeal { generation } => complete(
            seal_store.read_seal(generation).await,
            BootstrapEvent::SealRead,
        ),
        BootstrapEffect::PublishClaim(candidate) => complete(
            seal_store.publish_authority(&candidate).await,
            BootstrapEvent::ClaimPublished,
        ),
        BootstrapEffect::ReadTable { partition, table } => {
            complete(table_store.read_table(partition, &table).await, |decoded| {
                BootstrapEvent::TableRead { table, decoded }
            })
        }
        BootstrapEffect::ObserveWalTail => complete(
            wal_store.newest_surviving_batch().await,
            BootstrapEvent::WalTailObserved,
        ),
        BootstrapEffect::ReadWal { partition, batch } => complete(
            wal_store
                .read_wal(partition, batch)
                .await
                .map(|observed| observed.map(ObservedWal::into_object)),
            BootstrapEvent::WalRead,
        ),
        BootstrapEffect::EstablishFence(candidate) => complete(
            wal_store.establish_wal(&candidate).await,
            BootstrapEvent::FenceEstablished,
        ),
    }
}

fn complete<T>(
    result: Result<T, TypedStoreError>,
    succeeded: impl FnOnce(T) -> BootstrapEvent,
) -> BootstrapEvent {
    match result {
        Ok(value) => succeeded(value),
        Err(error) => BootstrapEvent::StoreFailed(error),
    }
}

fn mint_partition_id(entropy: &mut Entropy) -> PartitionId {
    let mut bytes = [0; 16];
    entropy.rng().fill_bytes(&mut bytes);
    if bytes == [0; 16] {
        bytes[15] = 1;
    }
    PartitionId::try_from(bytes).expect("partition entropy is deterministically made non-nil")
}
