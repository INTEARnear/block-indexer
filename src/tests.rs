use std::collections::{HashMap, HashSet};

use async_trait::async_trait;
use inindexer::{
    near_indexer_primitives::types::{AccountId, BlockHeight},
    neardata_server::NeardataServerProvider,
    run_indexer, BlockIterator, IndexerOptions, PreprocessTransactionsSettings,
};
use intear_events::events::tps::{
    block_info::BlockInfoEventData, moretps_claims::MoreTpsClaimEventData,
};
use tps_indexer::{TpsEventHandler, TpsIndexer};

#[derive(Default)]
struct TestIndexer {
    block_info: HashMap<BlockHeight, Vec<BlockInfoEventData>>,
    moretps_claims: HashMap<AccountId, Vec<MoreTpsClaimEventData>>,
}

#[async_trait]
impl TpsEventHandler for TestIndexer {
    async fn handle_block_info(&mut self, event: BlockInfoEventData) {
        self.block_info
            .entry(event.block_height)
            .or_default()
            .push(event);
    }

    async fn handle_moretps_claim(&mut self, event: MoreTpsClaimEventData) {
        self.moretps_claims
            .entry(event.round_account_id.clone())
            .or_default()
            .push(event);
    }
}

#[tokio::test]
async fn handles_block_info() {
    let mut indexer = TpsIndexer(TestIndexer::default());

    run_indexer(
        &mut indexer,
        NeardataServerProvider::mainnet(),
        IndexerOptions {
            range: BlockIterator::iterator(124099140..=124099140),
            preprocess_transactions: Some(PreprocessTransactionsSettings {
                prefetch_blocks: 0,
                postfetch_blocks: 0,
            }),
            ..Default::default()
        },
    )
    .await
    .unwrap();

    assert_eq!(format!("{:?}", indexer.0.block_info.get(&124099140)), "Some([BlockInfoEventData { block_height: 124099140, block_timestamp_nanosec: 1721769893724656286, block_hash: CEbNGstV7WNdMiydpF7m6FWcG8YpNU8CinhS9iKKtGey, block_producer: AccountId(\"fresh.poolv1.near\"), transaction_count: 123, receipt_count: 208 }])");
}

#[tokio::test]
async fn handles_moretps_claim() {
    let mut indexer = TpsIndexer(TestIndexer::default());

    run_indexer(
        &mut indexer,
        NeardataServerProvider::testnet(),
        IndexerOptions {
            range: BlockIterator::iterator(173542216..=173542225),
            preprocess_transactions: Some(PreprocessTransactionsSettings {
                prefetch_blocks: 0,
                postfetch_blocks: 0,
            }),
            ..Default::default()
        },
    )
    .await
    .unwrap();

    assert_eq!(
        indexer
            .0
            .moretps_claims
            .iter()
            .map(|(k, v)| (
                k,
                v.iter().filter(|claim| claim.is_success).count(),
                v.iter().filter(|claim| !claim.is_success).count()
            ))
            .collect::<HashSet<_>>(),
        HashSet::from_iter([
            (&"b.round-2.moretps.testnet".parse().unwrap(), 1676, 0),
            (&"z.round-2.moretps.testnet".parse().unwrap(), 1604, 0),
            (&"m.round-1.moretps.testnet".parse().unwrap(), 0, 57),
            (&"0.round-2.moretps.testnet".parse().unwrap(), 1461, 0),
            (&"h.round-2.moretps.testnet".parse().unwrap(), 1602, 0),
            (&"0.round-1.moretps.testnet".parse().unwrap(), 1, 503),
            (&"m.round-2.moretps.testnet".parse().unwrap(), 1694, 0),
            (&"h.round-1.moretps.testnet".parse().unwrap(), 0, 43),
            (&"b.round-1.moretps.testnet".parse().unwrap(), 0, 50),
            (&"z.round-1.moretps.testnet".parse().unwrap(), 0, 52)
        ])
    );
}
