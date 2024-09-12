# TPS Indexer

This indexer streams all blocks and MoreTPS events (a temporary events to test TPS) from the blockchain and pushes them to a Redis stream.

To run it, set `REDIS_URL` environment variable and `cargo run --release`
