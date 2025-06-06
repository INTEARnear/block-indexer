#[cfg(test)]
mod tests;

use block_indexer::{redis_handler::PushToRedisStream, BlockIndexer};
use inindexer::neardata::NeardataProvider;
use inindexer::{
    run_indexer, AutoContinue, BlockRange, IndexerOptions, PreprocessTransactionsSettings,
};
use redis::aio::ConnectionManager;

#[tokio::main]
async fn main() {
    dotenvy::dotenv().ok();
    simple_logger::SimpleLogger::new()
        .with_level(log::LevelFilter::Info)
        .with_module_level("inindexer::performance", log::LevelFilter::Debug)
        .init()
        .unwrap();

    let client = redis::Client::open(
        std::env::var("REDIS_URL").expect("No $REDIS_URL environment variable set"),
    )
    .unwrap();
    let connection = ConnectionManager::new(client).await.unwrap();

    let mut indexer = BlockIndexer(PushToRedisStream::new(connection, 1_000_000).await);

    run_indexer(
        &mut indexer,
        NeardataProvider::testnet(),
        IndexerOptions {
            preprocess_transactions: Some(PreprocessTransactionsSettings {
                prefetch_blocks: if cfg!(debug_assertions) { 0 } else { 100 },
                postfetch_blocks: 0,
            }),
            ..IndexerOptions::default_with_range(if std::env::args().len() > 1 {
                // For debugging
                let msg = "Usage: `indexer` or `indexer [start-block] [end-block]`";
                BlockRange::Range {
                    start_inclusive: std::env::args()
                        .nth(1)
                        .expect(msg)
                        .replace(['_', ',', ' ', '.'], "")
                        .parse()
                        .expect(msg),
                    end_exclusive: Some(
                        std::env::args()
                            .nth(2)
                            .expect(msg)
                            .replace(['_', ',', ' ', '.'], "")
                            .parse()
                            .expect(msg),
                    ),
                }
            } else {
                BlockRange::AutoContinue(AutoContinue::default())
            })
        },
    )
    .await
    .expect("Indexer run failed");
}
