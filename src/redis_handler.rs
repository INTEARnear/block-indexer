use async_trait::async_trait;
use inevents_redis::RedisEventStream;
use inindexer::near_indexer_primitives::types::BlockHeight;
use intear_events::events::block::info::BlockInfoEvent;
use redis::aio::ConnectionManager;

use crate::BlockEventHandler;

pub struct PushToRedisStream {
    block_info_stream: RedisEventStream<BlockInfoEvent>,
    max_stream_size: usize,
}

impl PushToRedisStream {
    pub async fn new(connection: ConnectionManager, max_stream_size: usize) -> Self {
        Self {
            block_info_stream: RedisEventStream::new(connection.clone(), BlockInfoEvent::ID),
            max_stream_size,
        }
    }
}

#[async_trait]
impl BlockEventHandler for PushToRedisStream {
    async fn handle_block_info(&mut self, event: BlockInfoEvent) {
        self.block_info_stream.add_event(BlockInfoEvent {
            block_height: event.block_height,
            block_hash: event.block_hash,
            block_timestamp_nanosec: event.block_timestamp_nanosec,
            transaction_count: event.transaction_count,
            receipt_count: event.receipt_count,
            block_producer: event.block_producer,
        });
    }

    async fn flush_events(&mut self, block_height: BlockHeight) {
        self.block_info_stream
            .flush_events(block_height, self.max_stream_size)
            .await
            .expect("Failed to flush block info stream");
    }
}
