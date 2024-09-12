use async_trait::async_trait;
use inevents_redis::RedisEventStream;
use intear_events::events::tps::{block_info::{BlockInfoEvent, BlockInfoEventData}, moretps_claims::{MoreTpsClaimEvent, MoreTpsClaimEventData}};
use redis::aio::ConnectionManager;

use crate::TpsEventHandler;

pub struct PushToRedisStream {
    block_info_stream: RedisEventStream<BlockInfoEventData>,
    moretps_claim_stream: RedisEventStream<MoreTpsClaimEventData>,
    max_stream_size: usize,
}

impl PushToRedisStream {
    pub async fn new(connection: ConnectionManager, max_stream_size: usize, testnet: bool) -> Self {
        Self {
            block_info_stream: RedisEventStream::new(
                connection.clone(),
                if testnet {
                    format!("{}_testnet", BlockInfoEvent::ID)
                } else {
                    BlockInfoEvent::ID.to_string()
                },
            ),
            moretps_claim_stream: RedisEventStream::new(
                connection.clone(),
                if testnet {
                    format!("{}_testnet", MoreTpsClaimEvent::ID)
                } else {
                    MoreTpsClaimEvent::ID.to_string()
                },
            ),
            max_stream_size,
        }
    }
}

#[async_trait]
impl TpsEventHandler for PushToRedisStream {
    async fn handle_block_info(&mut self, event: BlockInfoEventData) {
        self.block_info_stream
            .emit_event(event.block_height, event, self.max_stream_size)
            .await
            .expect("Failed to emit block info event");
    }

    async fn handle_moretps_claim(&mut self, event: MoreTpsClaimEventData) {
        self.moretps_claim_stream
            .emit_event(event.block_height, event, self.max_stream_size)
            .await
            .expect("Failed to emit moretps claim event");
    }
}
