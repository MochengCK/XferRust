**Summary**: A stability- and correctness-focused release for the BT engine — fixes a peer session leak, rate-limiter deadlock and uTP stream corruption, hardens against path traversal and metadata poisoning, and completes ut_metadata serving, tracker reporting and choking behavior.

## New Features

- Serve ut_metadata (BEP 9) requests: magnet peers get proper DATA/REJECT responses instead of being ignored
- `seed_duration` is now honored: seeding exits cleanly with a stopped announce when the configured time is up

## Bug Fixes

- Fix inbound peer session leak: passive connections were never unregistered, stalling their assigned pieces for up to 180 s
- Fix rate-limiter deadlock: token bucket guarantees at least one max-size block, limits below 16 KiB/s no longer freeze the connection
- Fix uTP stream corruption: read path applies back-pressure via channel capacity; write path buffers partially accepted bytes instead of dropping them
- Sanitize torrent paths (name / path segments) to prevent path-traversal writes outside the download directory
- Fix scheduler treating 0→0 throughput as a 100% decline — target connection count no longer collapses during cold start
- Fix choking algorithm: wall-clock rounds shared by all sessions, engine-level optimistic unchoke that actually reaches the lucky peer, round-robin uploads while seeding
- Fix tracker announces always reporting zero uploaded bytes (private-tracker ratio tracking works now)
- Fix UDP trackers never receiving stopped/completed events

## Other Changes

- Failed dial / session addresses are re-queued for a bounded number of retries instead of waiting for the next announce
- DHT hardening: known-peer table capped with FIFO eviction, inbound datagram processing concurrency limited
