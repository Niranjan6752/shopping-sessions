# Shopping Sessions: a Rust pipeline

Builders Buildathon, Databricks track. Turns the click-stream CSVs in `/data` into
`/output/user_daily/` and `/output/campaign_daily/` exactly as `rules.md` defines.

## How it works

One Rust binary, all stages parallel (rayon), input memory-mapped:

1. **Links**: parse, intern IDs into 256 hash shards, union-find, name each person by its byte-wise smallest ID.
2. **Events**: a hand-written parser for the exact CSV dialect (quotes, `\r`, field counts, ASCII-only digits,
   F1/F2/F3 timestamps) validates each row and buckets it by `event_id` hash.
3. **Dedup** per bucket keeps the copy with the smallest `ingest_ts`, then drops it if it is invalid.
4. **Persons** are resolved shard by shard so each lookup table stays in cache; events and clicks are
   regrouped by person.
5. **Per person**: sort by time, cut sessions at gaps over 30 min, credit each session to the latest click in the
   7 days up to its start (ties by smallest `click_id`), and count assists per campaign from merged
   `[click, click + 7 days]` intervals.
6. **Parquet** output is written in parallel, uncompressed.

IDs are kept as exact 16-byte values (their bytes when at most 15 long, else a 120-bit hash).

## Run locally

```sh
cargo build --release
DATA_DIR=dev OUTPUT_DIR=out target/release/shopping-sessions
python3 scripts/run_fixtures.py fixtures -- target/release/shopping-sessions   # 36 edge cases
python3 scripts/compare.py out dev-answer                                        # exact diff
```

## Open-source code used

Crates from crates.io: `rayon`, `memmap2`, `memchr`, `hashbrown`, `rustc-hash`, `bytes`, `parquet`
(Apache Arrow). The test scripts and fixtures come from the track's starter kit.

## License

MIT
