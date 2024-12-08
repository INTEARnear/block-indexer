pub mod redis_handler;

use async_trait::async_trait;
use inindexer::near_indexer_primitives::types::BlockHeight;
use inindexer::near_indexer_primitives::StreamerMessage;
use inindexer::Indexer;
use intear_events::events::block::info::BlockInfoEvent;

#[async_trait]
pub trait TpsEventHandler: Send + Sync {
    async fn handle_block_info(&mut self, event: BlockInfoEvent);

    /// Called after each block
    async fn flush_events(&mut self, block_height: BlockHeight);
}

pub struct TpsIndexer<T: TpsEventHandler + Send + Sync + 'static>(pub T);

#[async_trait]
impl<T: TpsEventHandler + Send + Sync + 'static> Indexer for TpsIndexer<T> {
    type Error = String;

    async fn process_block(&mut self, block: &StreamerMessage) -> Result<(), Self::Error> {
        self.0
            .handle_block_info(BlockInfoEvent {
                block_height: block.block.header.height,
                block_hash: block.block.header.hash,
                block_timestamp_nanosec: block.block.header.timestamp_nanosec as u128,
                transaction_count: block
                    .shards
                    .iter()
                    .filter_map(|shard| shard.chunk.as_ref())
                    .map(|chunk| chunk.transactions.len() as u64)
                    .sum(),
                receipt_count: block
                    .shards
                    .iter()
                    .filter_map(|shard| shard.chunk.as_ref())
                    .map(|chunk| chunk.receipts.len() as u64)
                    .sum(),
                block_producer: block.block.author.clone(),
            })
            .await;
        Ok(())
    }

    async fn process_block_end(&mut self, block: &StreamerMessage) -> Result<(), Self::Error> {
        self.0.flush_events(block.block.header.height).await;
        Ok(())
    }
}
