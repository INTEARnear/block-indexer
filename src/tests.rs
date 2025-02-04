use std::collections::HashMap;

use async_trait::async_trait;
use block_indexer::{BlockEventHandler, BlockIndexer};
use inindexer::{
    near_indexer_primitives::types::BlockHeight, neardata::NeardataProvider, run_indexer,
    BlockIterator, IndexerOptions, PreprocessTransactionsSettings,
};
use intear_events::events::block::info::BlockInfoEvent;

#[derive(Default)]
struct TestIndexer {
    block_info: HashMap<BlockHeight, Vec<BlockInfoEvent>>,
}

#[async_trait]
impl BlockEventHandler for TestIndexer {
    async fn handle_block_info(&mut self, event: BlockInfoEvent) {
        self.block_info
            .entry(event.block_height)
            .or_default()
            .push(event);
    }

    async fn flush_events(&mut self, _block_height: BlockHeight) {
        // No-op for tests
    }
}

#[tokio::test]
async fn handles_block_info() {
    let mut indexer = BlockIndexer(TestIndexer::default());

    run_indexer(
        &mut indexer,
        NeardataProvider::mainnet(),
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

    assert_eq!(format!("{:?}", indexer.0.block_info.get(&124099140)), "Some([BlockInfoEvent { block_height: 124099140, block_timestamp_nanosec: 1721769893724656286, block_hash: CEbNGstV7WNdMiydpF7m6FWcG8YpNU8CinhS9iKKtGey, block_producer: AccountId(\"fresh.poolv1.near\"), transaction_count: 123, receipt_count: 208 }])");
}
