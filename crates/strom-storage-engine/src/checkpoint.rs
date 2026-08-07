//! Checkpoint effects and their bounded storage pipelines.

mod collect;
mod prepare;

pub(crate) use collect::collect_advance;

use futures::{StreamExt as _, stream};
use strom_object_store::ObjectStoreAdapter;
use strom_storage_domain::{EncodedAuthoritySeal, EncodedTable};
use strom_storage_protocol::{PreparationOutcome, SealPublication, TypedStoreError};
use tokio::sync::{mpsc, oneshot};

use crate::store::{SealStore, TableEstablishment, TableStore};

use self::prepare::prepare_checkpoint;

const CHECKPOINT_CHILD_CREATES_MAX: usize = 16;
// Keep preparation behind the fixed-width child-create pipeline instead of
// retaining a checkpoint's encoded tables in aggregate.
const CHECKPOINT_TABLE_CHANNEL_MAX: usize = 1;

pub(crate) async fn prepare_checkpoint_effect(
    adapter: ObjectStoreAdapter,
    input: strom_storage_protocol::CheckpointInput,
) -> PreparationOutcome {
    let (table_sender, table_receiver) = mpsc::channel(CHECKPOINT_TABLE_CHANNEL_MAX);
    let (prepared_sender, prepared_receiver) = oneshot::channel();
    let preparation = tokio::task::spawn_blocking(move || {
        let prepared = prepare_checkpoint(input, &mut |table| {
            table_sender.blocking_send(table).is_ok()
        });
        let _consumer_may_be_gone = prepared_sender.send(prepared);
    });

    let store = TableStore::new(adapter);
    let table_result = establish_tables(&store, table_receiver).await;
    if let Err(join_error) = preparation.await {
        return PreparationOutcome::Contradiction {
            detail: format!("checkpoint preparation task failed: {join_error}"),
        };
    }
    match table_result {
        TableEstablishment::Established => {}
        TableEstablishment::Abandoned => return PreparationOutcome::Abandoned,
        TableEstablishment::Contradiction { detail } => {
            return PreparationOutcome::Contradiction { detail };
        }
    }
    match prepared_receiver.await {
        Ok(Ok(Some(prepared))) => PreparationOutcome::Prepared(prepared),
        Ok(Ok(None)) => PreparationOutcome::Abandoned,
        Ok(Err(error)) => PreparationOutcome::Contradiction {
            detail: error.to_string(),
        },
        Err(_sender_dropped) => PreparationOutcome::Contradiction {
            detail: "checkpoint preparation ended without a result".into(),
        },
    }
}

pub(crate) async fn publish_authority(
    adapter: ObjectStoreAdapter,
    candidate: EncodedAuthoritySeal,
) -> Result<SealPublication, TypedStoreError> {
    SealStore::new(adapter).publish_authority(&candidate).await
}

async fn establish_tables(
    store: &TableStore,
    mut receiver: mpsc::Receiver<EncodedTable>,
) -> TableEstablishment {
    let tables = stream::poll_fn(move |context| receiver.poll_recv(context));
    let creates = tables
        .map(|table| {
            let store = store.clone();
            async move { store.establish_table(&table).await }
        })
        .buffer_unordered(CHECKPOINT_CHILD_CREATES_MAX);
    futures::pin_mut!(creates);
    loop {
        match creates.next().await {
            Some(TableEstablishment::Established) => {}
            Some(TableEstablishment::Abandoned) => return TableEstablishment::Abandoned,
            Some(TableEstablishment::Contradiction { detail }) => {
                return TableEstablishment::Contradiction { detail };
            }
            None => return TableEstablishment::Established,
        }
    }
}
