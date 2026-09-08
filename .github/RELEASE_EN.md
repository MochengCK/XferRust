**Summary**: A release built around two themes — magnet download experience and BT stability on real-world networks. Magnet links resolve into a file table for selective download (nothing is downloaded until you confirm); the engine gains Tracker subscriptions and full BT seeding lifecycle management (share-ratio auto-stop, continuous seeding); BT discoverability is completed — local peer discovery (LPD), automatic UPnP/NAT-PMP port mapping and configurable BT/DHT listen ports; a batch of protocol-correctness issues is fixed (inbound session leak, rate-limiter deadlock, uTP stream corruption, path traversal and metadata poisoning), the HTTP/HTTPS tail-end stall that hung at 99% is gone, and BT scheduling parameters are re-tuned from real-network measurements. The TUI is fully redesigned — the add-task dialog supports a per-task download directory and instant magnet parsing, and the main layout is rebuilt into a modern flat design. This release also ships a batch of protocol and query capabilities: per-second sliding-window speed sampling, BT piece bitfield and per-peer bitfield exposure, BT peer IP banning (timed/permanent), delete-files-on-remove, command-line runtime option passthrough, and two aria2-compatible RPCs `task.changeUri` / `task.getServers`. Android (arm64-v8a) engine-core cross-compilation is now supported.

## New Features

- Magnet file selection: the engine starts parsing metadata as soon as a magnet link is added; once parsed, the task pauses and a file table is shown (checkbox, file size, selection summary), and only the checked files are downloaded. Pieces spanning the selected/unselected boundary are downloaded whole, as BitTorrent requires
- Magnet tasks are resumable: a task still waiting for file selection reopens the file table after a restart instead of silently downloading everything

### Tracker Subscriptions

- New tracker subscriptions: subscribe to a remote URL that returns one tracker per line; refreshed automatically on a 24-hour TTL, applied immediately to all BT tasks
- Multi-subscription sync semantics: trackers dropped from a subscription's remote source are removed, unless they are still provided by another subscription or added manually; source bookkeeping is fully exposed via `engine.getOptions` / the subscription API
- Full-replacement global tracker list: `bt-tracker` in `engine.changeOptions` accepts an array or a newline/comma-separated string and syncs incrementally to all running tasks
- Subscriptions can be added, removed, enabled/disabled and refreshed (single/all); subscriptions and tracker sources are persisted with the session
- New `auto-update-trackers` switch controls automatic subscription refresh

### BT Seeding Lifecycle Management

- BT tasks can be configured to enter a seeding state after download completion (instead of finishing immediately), controlled by the global option `bt-seed-mode` (`true`/`false`)
- Seeding tasks display a live share ratio (uploaded / downloaded); a target share ratio `bt-seed-ratio` (e.g. `1.5`, `2.0`) can be set — seeding auto-stops when the ratio is reached
- Seeding tasks can be paused/resumed, and seeding can be manually stopped (RPC `task.stopSeed` / TUI `S` key)
- Continuous seeding until manual stop is supported (`bt-seed-ratio` of `0` disables auto-stop)
- TUI settings page adds seeding mode and target share ratio options; task list/detail views show upload speed and share ratio for seeding tasks

### BT Network Discovery & Port Configuration

- Local peer discovery (LPD / BEP 14): each BT task joins the LSD multicast group, periodically announcing to and listening on the LAN, so peers on the same network can find each other without trackers; discovered peers enter scheduling with an `lpd` source
- Automatic UPnP / NAT-PMP port mapping: when a BT task starts, the engine sets up a TCP + UDP dual mapping on the router (2-hour lease, renewed every 1/3 of the lease, automatic retry next cycle after a single failure) and best-effort removes the mapping on shutdown — better connectability, no more manual router port forwarding
- Configurable BT / DHT listen ports: new global options `bt-listen-port` / `dht-listen-port` (`0` = random port), applied when a BT task is created; if the listen port or DHT port is taken, the engine falls back to an ephemeral port with a warning instead of disabling listening entirely
- New switches `bt-enable-lpd` / `bt-port-mapping` (both on by default) to disable either feature independently; `engine.getOptions` exposes the new options and `changeGlobalOption` validates port values (0–65535)
- The TUI settings page gains the four matching rows: BT / DHT listen port (Enter to edit, validated 0–65535, `0` shown as "random") plus Local discovery and Port mapping toggles (←/→ to switch); conns-per-server, min split size and language shift below them

### Piece Display, Real-Time Speed & Peer Management

- Real-time download speed: speed sampling changed from "a 3-second window updated every 3 seconds" to a **3-second sliding window refreshed every second** — window averaging still smooths the 0↔spike jitter caused by piece-batch disk flushes, but the speed value now updates every second instead of freezing for seconds at a time
- BT piece bitfield exposure: the `bitfield` field in task status responses (`task.tell` / `task.list`) is no longer an empty string — it now carries the real "completed pieces" bitmap (aria2-compatible hex encoding: 1 bit per piece, MSB-first within each byte); synced at 1 Hz by the download driver and preserved across pauses, so clients can render a piece progress map
- Per-peer bitfield: `task.getPeers` now includes a `bitfield` field per peer (hex bitmap of pieces the peer owns, all-ones for seeds), enabling per-peer piece distribution views
- BT peer IP banning: new RPC `task.banPeer` (auto-unbans after `duration` seconds, `<= 0` means permanent; banning immediately disconnects existing connections from that IP and purges it from the pending queue) and `task.unbanPeer` (global semantics, applies to all BT tasks); the ban list is persisted with the session and survives restarts
- Global ban-list option: `engine.changeOptions` accepts a new `bt-ip-ban-list` key (array of IPs, or a newline/comma-separated string; full-replacement semantics for permanent bans) so app preferences can be forwarded directly
- HTTP task URI change: new RPC `task.changeUri` (aria2-compatible semantics: for waiting/paused tasks, removes `delUris` and appends `addUris` for the given `fileIndex`; active tasks are rejected and the client falls back to re-creating the task)
- Server list: new RPC `task.getServers` (HTTP tasks return aria2-compatible server entries with `currentUri` / `downloadSpeed` / `downloadLength`; BT tasks return an empty array)

### Command-Line Runtime Option Passthrough

- The engine command line now accepts `--key=value` runtime global options injected into the same store as `engine.changeOptions`: `split`, `max-connection-per-server`, `min-split-size`, overall rate limits, `bt-max-peers`, `bt-adaptive`, `bt-seed-mode`, `bt-seed-ratio`, `bt-encryption`, `bt-protocol`, `bt-listen-port`, `dht-listen-port`, `bt-enable-lpd`, `bt-port-mapping`
- App-side switches map automatically: `--enable-upnp` / `--enable-nat-pmp` → `bt-port-mapping`, `--enable-utp` → `bt-protocol` (`tcp+utp` / `tcp`) — host applications no longer need a second RPC round-trip after launching the engine

### Android Engine-Core Build

- Added Android (aarch64-linux-android / arm64-v8a) cross-compilation support — builds only the engine core (`xferrust`), no TUI
- `Cargo.toml` makes TUI dependencies (crossterm, ratatui, etc.) optional under a `tui` feature; `default = ["tui"]`; the `xfer` binary is marked `required-features = ["tui"]`
- CI adds a `build-android` job: cross-compiles with NDK r27c + API 24 (Android 7.0+); artifact `xferrust-android-arm64-v8a.tar.gz` is included in releases
- New local build script `scripts/build-android.sh`: auto-detects NDK host-tag (macOS / Linux), sets CC/CXX/AR/Linker env vars and invokes `cargo build --no-default-features --bin xferrust`

### Linux ARM64 build

- CI build matrix adds `linux-arm64` (aarch64-unknown-linux-musl): cross-compiled on an ubuntu x64 runner with the musl toolchain; artifacts `xfer-tui-linux-arm64.tar.gz` / `xferrust-linux-arm64.tar.gz` are included in releases
- Statically linked against musl: no glibc version dependency, no bundled lib/ directory needed — extract and run

### TUI Redesign

- Redesigned add-task dialog: URL + directory fields; an empty directory falls back to the global download directory (per task only). The directory can be typed manually or picked with the native system folder dialog by pressing Enter (macOS / Windows / Linux)
- Magnet links start parsing metadata immediately, no extra confirmation step; the dialog shows live parsing progress (connections / waiting / elapsed). Once metadata is ready, the dialog expands downward into a file-selection table: Space to toggle, A to select all/none, Enter to start downloading the selected files
- The download directory in Settings is also picked with the native system folder dialog
- Task detail pages (BT / non-BT) keep the global info bar at the top (brand logo + global speed / task counters) instead of hiding it

## Bug Fixes

- Fix magnet tasks reporting an empty file list while downloading: metadata is back-filled as soon as it is ready, so `files` and `numPieces` are no longer zero
- Fix magnet metadata being lost across restarts: the session persists the info dictionary and rebuilds the metadata from it, no need to re-fetch from peers
- Fix inbound peer session leak: passive connections were never unregistered, stalling their assigned pieces for up to 180 s
- Fix rate-limiter deadlock: token bucket guarantees at least one max-size block, limits below 16 KiB/s no longer freeze the connection
- Fix uTP stream corruption: read path applies back-pressure via channel capacity; write path buffers partially accepted bytes instead of dropping them
- Sanitize torrent paths (name / path segments) to prevent path-traversal writes outside the download directory
- Fix scheduler treating 0→0 throughput as a 100% decline — target connection count no longer collapses during cold start
- Fix choking algorithm: wall-clock rounds shared by all sessions, engine-level optimistic unchoke that actually reaches the lucky peer, round-robin uploads while seeding
- Fix tracker announces always reporting zero uploaded bytes (private-tracker ratio tracking works now)
- Fix UDP trackers never receiving stopped/completed events
- Fix the TUI subscription refresh not updating the task-side tracker list
- Fix HTTP/HTTPS downloads hanging at 99% for over ten seconds before finishing:
  - Tail short reads (response body shorter than the requested range) now use a dedicated backoff curve (from 250 ms, capped at 1 s) instead of sharing the 0.5–2 s hard-failure backoff — short reads always make progress when retried from the watermark, and the old backoff accumulated into a long tail under retry storms
  - New 10-second read-idle watchdog: a silently stalled connection (dead peer / half-open socket) resumes from the watermark immediately instead of waiting out the 30-second read timeout
  - Read timeouts are now classified as renewable transients: they no longer consume the 4-strike failure budget, so tail retries are no longer killed prematurely

## BT Scheduling Tuned for Real-World Networks

- Stagnation window widened from 2 rounds (20 s) to 6 rounds (60 s): on real networks a peer typically needs 20–40 s from connect to first data (handshakes + waiting for a choking round); the old setting caused a connect/disconnect churn where speeds never took off
- Slow-peer eviction tightened: median-relative ratio 0.25 → 0.1, absolute floor 1 KB/s → 10 KB/s — only near-zero-contribution peers get evicted; slow but working peers are no longer killed
- Grace period 15 s → 40 s and a lower per-round eviction ratio: one-shot mass evictions used to release many pieces at once, causing wasteful reassignment churn
- Added BT speed analysis and public-network measurement tooling (`ANALYSIS_BT_SPEED.md`, `scripts/bt_public_test.py`, `scripts/bt_real_test.py`); scheduling parameters are now grounded in measured data

## Other Changes

- Task removal can now delete downloaded files and control files: `task.remove` accepts an optional `deleteFiles` parameter (default `false`, preserving the original semantics)
- New file-selection API: `select_files(gid, file_indices)` / `get_selected_files(gid)`, applied live on running tasks; `add_uri` accepts a `bt-file-selection` option to pause a magnet task after parsing and wait for a selection
- Task status exposes a new `awaitingSelection` field, and `files[].selected` now reflects the real selection
- New seeding status `seeding` and `seedRatio` field; RPC adds `task.stopSeed` method; session save/restore handles seeding tasks correctly (serialized as `waiting` so they re-download on restart)
- "Is the file complete?" checks now use on-disk allocated size (`st_blocks × 512`) as a lower bound: BT's random writes naturally produce sparse files, and logical length would misjudge hole-y files as complete and seed corrupted data
- Failed dial / session addresses are re-queued for a bounded number of retries instead of waiting for the next announce
- A short-read storm exceeding 8 consecutive retries folds into one regular failure under the normal failure budget — the relaxed backoff does not slow down error reporting for permanently broken servers
- Added HTTP tail-truncation + pause/resume regression tests (short-read storms, silent stalls, cancel & resume scenarios)
- DHT hardening: known-peer table capped with FIFO eviction, inbound datagram processing concurrency limited
- Added 27 TUI rendering regression tests (border geometry, column alignment, dialog flow, detail top bar, etc.)
