**Summary**: Building on v0.3.0, this release continues along three themes — completing HTTP download capabilities, frontend protocol integration, and observability. HTTP downloads gain real piece bitmaps on par with BT (`numPieces` / `pieceLength` / `bitfield`, advancing live with bytes landed) and a global rate limiter (cross-task shared token bucket, `1M`/`500K` units accepted, hot-reloaded at runtime); `task.getTrackers` is upgraded with per-tracker announce state (protocol / status / seeders / next announce time). On the protocol-integration side: task status now carries an aria2-style `bittorrent` object and an `infoHash` field (frontend BT detection and task naming depend on them), command-line runtime options are passed through (`--key=value`, with app-side toggles like `--enable-upnp` auto-mapped), and `engine.changeOptions` supports full tracker-list replacement (`bt-trackers`) plus a subscription auto-update switch. On the observability side: download speed now refreshes every second via a 3-second sliding window, BT piece bitfields and per-peer bitfields are exposed, BT peer IP banning (timed/permanent) is added, along with two aria2-compatible RPCs `task.changeUri` / `task.getServers`. CI gains a linux-arm64 (aarch64 musl static) build matrix.

## New Features

### Task BT Identity (bittorrent / infoHash)

- Task status responses (`task.tell` / `task.list`, both the aria2-style and native numeric encodings) now carry a `bittorrent` object and an `infoHash` field: non-BT tasks report `bittorrent` as `null` (the frontend detects BT tasks by the truthiness of `task.bittorrent`); when metadata is ready (.torrent / magnet metadata fetched) it is `{"info": {"name", "hash"}}`; while magnet metadata is still being fetched it is `{}` (the frontend shows "fetching metadata" accordingly)
- `infoHash` is the hex encoding of the info dictionary hash: .torrent tasks report the hash computed when the metadata was parsed, magnet tasks report the `bt_info_hash` obtained from the handshake / metadata exchange

### Global Tracker List & Subscription Switch Passthrough

- `engine.changeOptions` gains `bt-trackers`: accepts a string array or a newline/comma-separated string, with full-replacement semantics (the app pushes the complete list each time); synchronized to all active BT tasks with the same delta semantics as manual add/remove (added URLs injected, removed URLs pruned), sourced as `manual`
- New switch `auto-update-trackers` controls tracker subscription auto-update (boolean semantics; `"false"` / `"0"` both mean off)

### Command-Line Runtime Option Passthrough

- The engine command line now accepts `--key=value` runtime global options injected into the same store as `engine.changeOptions` (CLI values override same-named session-restored values): `split`, `max-connection-per-server`, `min-split-size`, overall rate limits, `bt-max-peers`, `bt-adaptive`, `bt-seed-mode`, `bt-seed-ratio`, `bt-encryption`, `bt-protocol`, `bt-listen-port`, `dht-listen-port`, `bt-enable-lpd`, `bt-port-mapping`
- App-side toggles are auto-mapped: `--enable-upnp` / `--enable-nat-pmp` → `bt-port-mapping`, `--enable-utp` → `bt-protocol` (`tcp+utp` / `tcp`), so the host application no longer needs a second round of RPC dispatch at engine startup

### Piece Display, Real-Time Speed & Peer Management

- Real-time download speed: speed sampling has changed from "a 3-second window average updated every 3 seconds" to a **3-second sliding window refreshed every second** — the window average still smooths out the 0↔spike jitter caused by piece-level batched writes, but the speed value now updates every second, so the frontend no longer shows a speed number frozen for long stretches
- BT piece bitfield exposure: the `bitfield` field in task status responses (`task.tell` / `task.list`) is no longer always an empty string — it reports the real "downloaded pieces" bitmap (aria2-compatible hex encoding: 1 bit per piece, high bit first within each byte); the driver syncs at 1 Hz and the last known state is preserved while paused, letting clients render piece progress maps
- Per-peer bitfield: each peer in `task.getPeers` gains a `bitfield` field (hex bitmap of the pieces the peer owns, all ones for seeds), enabling per-peer piece distribution rendering
- BT peer IP banning: new RPCs `task.banPeer` (auto-unbans after `duration` seconds, `<= 0` means permanent; banning immediately disconnects the IP's existing connections and clears pending dials) / `task.unbanPeer` (global semantics, applies to all BT tasks); the ban list persists across sessions and stays effective after restart
- Global ban-list option: `engine.changeOptions` gains `bt-ip-ban-list` (IP array or newline/comma-separated string, full replacement of permanent bans), passable from the app's preference settings
- HTTP task URI change: new RPC `task.changeUri` (aria2-compatible semantics: for waiting/paused tasks, removes `delUris` and appends `addUris` for the given `fileIndex`; active tasks are rejected and the client falls back to re-creating the task)
- Server list: new RPC `task.getServers` (HTTP tasks return aria2-compatible server entries with `currentUri` / `downloadSpeed` / `downloadLength`; BT tasks return an empty array)

### HTTP Piece Bitmap & Global Rate Limiting

- HTTP task piece bitmap: `task.tell` / `task.list` (both the aria2-style and native numeric encodings) now report real piece data for HTTP tasks — `numPieces` / `pieceLength` / `bitfield` (aria2-compatible hex encoding). Piece length follows `min-split-size` (same granularity as the split segments); the write side accounts bytes incrementally per landed range, covering both the multi-connection writer thread and the single-connection sequential path; the bitmap is invalidated and rebuilt when a server ignores Range and resends the full body; the control-file watermark pre-fills the bitmap on resume so it stays consistent with what is actually on disk; omitted when total length is unknown or Range is unsupported; the last known state is preserved while paused. Previously HTTP tasks always reported `numPieces=0` / an empty `bitfield`, leaving piece maps blank in the task list and detail views
- HTTP global rate limiting: a new cross-task shared asynchronous token-bucket limiter (`RateLimiter`) is injected into every download connection — multi-connection workers consume tokens in their read loops and the single-connection path consumes per chunk before writing; when tokens run out the connection awaits asynchronously, letting TCP backpressure converge naturally. `engine.changeOptions` hot-updates take effect immediately. Previously the limits were only forwarded to BT engines — HTTP downloads were entirely unthrottled
- Speed-limit value parsing upgraded: `max-overall-download-limit` / `max-overall-upload-limit` accept aria2-style units (`1M` / `500K` / plain byte integers), matching the format the desktop stores (previously only plain integers were accepted; unit-suffixed values were rejected or silently treated as unlimited)
- One invalid key no longer aborts the whole settings batch: in `changeOptions`, illegal values for speed limits / `bt-encryption` / `bt-protocol` / port options degrade to a warning and skip that key while the rest of the batch applies as usual — previously a single bad key made the whole changeOptions call fail, so saving one speed limit would break every other system setting
- Tracker announce state: `task.getTrackers` is upgraded from URL-only entries to per-tracker state — `protocol` (http / https / udp / ws), `status` (working / not-working / waiting), `seeders` / `leechers` (the tracker-reported complete / incomplete), `peers`, `lastAnnounceTime` / `nextAnnounceTime` (derived from the successful response interval) and `error` (the latest failure reason); the BT engine records per-URL results while aggregating each announce round, and URLs not yet announced stay `waiting`

## Bug Fixes

- Fix HTTP tasks falsely reporting `seeder=true` on completion: `seeder` now means "this endpoint is a BT task and its payload is complete"; it used to be computed as "completed ≥ total" for every task type, so finished HTTP tasks tripped the flag and the client marked normal downloads as "seeding" and re-emitted the BT completion event
- Fix HTTP tasks always reporting empty piece data (`numPieces=0` / empty `bitfield`); see "HTTP Piece Bitmap & Global Rate Limiting"
- Fix speed-limit settings not taking effect: unit-suffixed values (e.g. `1M`) were rejected or silently treated as unlimited, and the HTTP download path had no rate-limit enforcement at all; see "HTTP Piece Bitmap & Global Rate Limiting"

## Build & Release

- CI build matrix gains `linux-arm64` (aarch64-unknown-linux-musl): artifacts `xfer-tui-linux-arm64.tar.gz` / `xferrust-linux-arm64.tar.gz` are published with each Release
- musl static linking: no glibc version dependency and no bundled lib/ directory needed — extract and run
- linux-arm64 cross-compilation now uses cargo-zigbuild, replacing the unreliable musl.cc download source
