pub mod redis_handler;

use async_trait::async_trait;
use inindexer::near_indexer_primitives::{
    views::{ActionView, ReceiptEnumView},
    StreamerMessage,
};
use inindexer::{IncompleteTransaction, Indexer, TransactionReceipt};
use intear_events::events::tps::{
    block_info::BlockInfoEventData, moretps_claims::MoreTpsClaimEventData,
};

#[async_trait]
pub trait TpsEventHandler: Send + Sync {
    async fn handle_block_info(&mut self, event: BlockInfoEventData);
    async fn handle_moretps_claim(&mut self, event: MoreTpsClaimEventData);
}

pub struct TpsIndexer<T: TpsEventHandler + Send + Sync + 'static>(pub T);

#[async_trait]
impl<T: TpsEventHandler + Send + Sync + 'static> Indexer for TpsIndexer<T> {
    type Error = String;

    async fn process_block(&mut self, block: &StreamerMessage) -> Result<(), Self::Error> {
        self.0
            .handle_block_info(BlockInfoEventData {
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

    async fn on_receipt(
        &mut self,
        receipt: &TransactionReceipt,
        transaction: &IncompleteTransaction,
        block: &StreamerMessage,
    ) -> Result<(), Self::Error> {
        if receipt
            .receipt
            .receipt
            .receiver_id
            .as_str()
            .ends_with(".moretps.near")
            || receipt
                .receipt
                .receipt
                .receiver_id
                .as_str()
                .ends_with(".moretps.testnet")
        {
            if let ReceiptEnumView::Action {
                signer_id, actions, ..
            } = &receipt.receipt.receipt.receipt
            {
                for action in actions.iter() {
                    if let ActionView::FunctionCall { method_name, .. } = action {
                        if method_name == "claim" {
                            let event = MoreTpsClaimEventData {
                                block_height: block.block.header.height,
                                block_timestamp_nanosec: block.block.header.timestamp_nanosec
                                    as u128,
                                receipt_id: receipt.receipt.receipt.receipt_id,
                                transaction_id: transaction.transaction.transaction.hash,
                                claimed_account_id: signer_id.clone(),
                                claimed_parent_account_id: if let Some((_child, parent)) =
                                    signer_id.as_str().split_once(".")
                                {
                                    parent.parse().unwrap()
                                } else {
                                    "tla".parse().unwrap()
                                },
                                round_account_id: receipt.receipt.receipt.receiver_id.clone(),
                                round_parent_account_id: if let Some((_child, parent)) =
                                    receipt.receipt.receipt.receiver_id.as_str().split_once(".")
                                {
                                    parent.parse().unwrap()
                                } else {
                                    "tla".parse().unwrap()
                                },
                                is_success: receipt.is_successful(false),
                            };
                            self.0.handle_moretps_claim(event).await;
                        }
                    }
                }
            }
        }
        Ok(())
    }
}
