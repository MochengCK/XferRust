**Summary**

- New disk cache `disk-cache` (piece-level write-back buffering), magnet save/load as torrent (`bt-save-metadata` / `bt-load-saved-metadata`)
- Peer-discovery switches now really take effect: `enable-dht` / `enable-dht6` (BEP 32, DHT over IPv6 dual-stack) / `enable-peer-exchange` (BEP 11)
- HTTP client configuration takes effect: `continue` resume switch, `user-agent`, `all-proxy` / `no-proxy` proxy (client rebuilt on change)
- New task file-verification RPC `task.verifyFiles` (existence / size / streaming hash, executed natively by the engine)
- UDP hole punching (`ut_holepunch`) aligned with the libtorrent standard — relays through double NATs with standard clients such as qBittorrent
- HTTP piece bitmaps, global & per-task rate limiting, per-task average speed, command-line option passthrough
- File selection (`select-file`) works end-to-end; `.torrent` additions support the `bt-file-selection` flow
- Piece display upgrades: persisted tri-state bitmap (`partialBitfield`), wanted-piece bitmap `wantedBitfield`, seed duration `bt-seed-time` that stops seeding automatically when reached; BT peer IP banning and `task.getPeers` per-peer stats / banned group
- Fixed phantom progress on unselected files and task progress above 100%: per-file and task progress now share one piece-segment accounting
- Fixed magnet/torrent tasks skipping "awaiting file selection" and downloading directly, garbled Chinese task names, inaccurate average speed
- All non-terminal tasks are restored as paused after restart; per-task rate limits now override the global one

## New Features

### Disk Cache (disk-cache)

- New global option `disk-cache` (bytes or K/M/G suffix such as `128M`; `0` = write-through off): BT piece data is first buffered in a memory write-back queue and flushed to disk in FIFO order, reducing small random writes
- Read paths serve cached pieces directly from memory without touching disk; `flush_all` flushes the cache before syncing files, so the resume bitmap is only persisted after its data is on disk
- Pieces larger than the cache limit degrade to direct writes; rewriting the same piece replaces the old entry without double counting; invalid values degrade to a warning without aborting the batch

### Magnet Save / Load as Torrent (bt-save-metadata / bt-load-saved-metadata)

- New global option `bt-save-metadata`: when magnet metadata arrives it is written to `<download-dir>/<hex-infohash>.torrent` (top-level dictionary hand-encoded with the original info bytes so the info_hash never shifts; atomic write via temp file + rename)
- New global option `bt-load-saved-metadata`: on magnet task start, if a same-named `.torrent` exists in the download dir with a matching info_hash, the metadata is installed directly, skipping the peer fetch (magnet becomes a torrent task instantly)
- Tasks created from a `.torrent` file are not re-saved (the original file already exists)

### Peer-Discovery Switches (enable-dht / enable-dht6 / enable-peer-exchange)

- New global option `enable-dht` (DHT, BEP 5): on by default; when off, BT tasks no longer join the DHT; private torrents never use DHT (the constraint wins)
- New global option `enable-dht6` (DHT over IPv6, BEP 32): when on, DHT binds a dual-stack socket (`[::]`), bootstraps IPv6 nodes, replies to IPv6 requesters with `nodes6` / `peers6` and parses remote v6 nodes; if the machine has no IPv6 stack and binding fails, it automatically falls back to IPv4
- New global option `enable-peer-exchange` (PEX, BEP 11): on by default; when off the extension handshake does not advertise `ut_pex` and PEX messages are neither sent nor received

### HTTP Client Configuration (continue / user-agent / all-proxy / no-proxy)

- New global option `continue` (HTTP resume switch, default on): when off, downloads always start from scratch even if the server supports Range; an existing file is kept and a new name is used (aria2 semantics)
- New global options `user-agent` / `all-proxy` / `no-proxy`: the HTTP client is built from them (UA overrides the engine default, proxy is direct or via `all-proxy`); changing them rebuilds the client so new connections pick it up immediately
- Desktop proxy settings (system / custom) and UA settings now really apply to download traffic

### Task BT Identity (bittorrent / infoHash)

- Task status responses (`task.tell` / `task.list`, both the aria2-style and native numeric encodings) now carry a `bittorrent` object and an `infoHash` field
- Non-BT tasks report `bittorrent` as `null`; when metadata is ready it is `{"info": {"name", "hash"}}`; while magnet metadata is being fetched it is `{}` (the frontend shows "fetching metadata")
- `infoHash` is the hex encoding of the info dictionary hash: .torrent tasks report the hash computed at parse time, magnet tasks report the `bt_info_hash` from the handshake / metadata exchange

### Global Tracker List & Subscription Switch Passthrough

- `engine.changeOptions` gains `bt-trackers`: a string array or newline/comma-separated string with full-replacement semantics, synchronized to all active BT tasks with the same delta semantics as manual add/remove
- New switch `auto-update-trackers` controls tracker subscription auto-update (`"false"` / `"0"` both mean off)

### Command-Line Runtime Option Passthrough

- The engine command line accepts `--key=value` runtime global options injected into the same store as `engine.changeOptions` (CLI values override session-restored ones): `split`, `max-connection-per-server`, `min-split-size`, overall rate limits, `bt-max-peers`, `bt-adaptive`, `bt-seed-mode`, `bt-seed-ratio`, `bt-encryption`, `bt-protocol`, `bt-listen-port`, `dht-listen-port`, `bt-enable-lpd`, `bt-port-mapping`
- App-side toggles are auto-mapped: `--enable-upnp` / `--enable-nat-pmp` → `bt-port-mapping`, `--enable-utp` → `bt-protocol`
- Any unknown `--key=value` is accepted as a global default (see Behavior Changes), so host applications can pass their entire configuration through the command line

### Piece Display, Real-Time Speed & Peer Management

- Download speed now uses a **3-second sliding window refreshed every second**: the window average smooths piece-level write jitter while the value updates every second
- BT piece bitfield exposure: `bitfield` reports the real "downloaded pieces" bitmap (aria2-compatible hex encoding), synced at 1 Hz, preserving the last known state while paused
- Per-peer bitfield: each peer in `task.getPeers` gains a `bitfield` field (pieces the peer owns, all ones for seeds)
- BT peer IP banning: `task.banPeer` (auto-unbans after `duration` seconds, `<= 0` permanent; immediately disconnects existing connections) / `task.unbanPeer`; the ban list persists across sessions
- Global ban-list option: `engine.changeOptions` gains `bt-ip-ban-list` (IP array or newline/comma-separated string, full replacement of permanent bans)
- New `task.changeUri` (aria2-compatible: removes `delUris` / appends `addUris` per `fileIndex` for waiting/paused tasks, rejected when active) and `task.getServers` (HTTP tasks return server entries, BT returns an empty array)

### Per-Task Average Speed (averageSpeed)

- Task status responses gain an `averageSpeed` field (bytes/sec): accumulated every second during active download phases; seeding and paused phases neither accumulate nor dilute it
- The accumulated data persists across sessions, so the average does not drift after restart-and-resume; the app reads this field directly instead of sampling

### File Selection (select-file) End-to-End

- `task.changeOption` now actually applies `select-file` (aria2 semantics: comma-separated 1-based file indices, empty = select all): running BT tasks hot-apply it, paused/waiting tasks pick it up on their next start
- `files[].selected` reports the real selection: the native encoding previously hardcoded `true`; the aria2 encoding now outputs `"true"` / `"false"`
- File selection extends to HTTP/HTTPS tasks: single-file layout, persisted and applied on the next start
- `task.add` accepts `select-file` for pre-selection; invalid/out-of-range values degrade to a warning without aborting the addition
- `.torrent` additions support the `bt-file-selection` flow: the same state machine as magnets — auto-pause once metadata is ready, resume via `select-file` after the client confirms the selection
- Single-file magnets auto-resume: when the layout is a single file, the engine resumes at full selection without user action

### HTTP Piece Bitmap & Global Rate Limiting

- HTTP tasks report real piece data: `numPieces` / `pieceLength` / `bitfield` (aria2-compatible hex encoding)
- Piece length is a display granularity decoupled from the segment granularity: `min(min-split-size, max(total/2048, 64KB))` — even a few-MB file lights up pieces promptly, large files keep the `min-split-size` granularity
- The write side accounts bytes incrementally per landed range on both multi-connection and single-connection paths; the bitmap is rebuilt when a server ignores Range and resends the full body; control-file watermarks pre-fill the bitmap on resume so it stays consistent with disk; omitted when total length is unknown or Range is unsupported
- HTTP global rate limiting: an asynchronous token bucket is injected into every download connection, letting TCP backpressure converge naturally. Previously HTTP downloads were entirely unthrottled
- Speed-limit value parsing upgraded: overall limits accept aria2-style units (`1M` / `500K` / plain bytes), previously only plain integers
- One invalid key no longer aborts the whole settings batch: illegal values in `changeOptions` degrade to a warning and skip that key

### Per-Task Rate Limiting (max-download-limit / max-upload-limit)

- `task.changeOption` accepts `max-download-limit` / `max-upload-limit` (aria2 semantics, `1M`/`500K` units pass through), covering HTTP and BT, hot-reloaded and persisted across sessions
- The effective value follows per-task-override semantics (see Behavior Changes); `task.getOption` reports the current per-task limits
- Each HTTP task owns a limiter shared by the single-connection and split paths; BT engines receive the composed values directly

### Tracker Announce State

- `task.getTrackers` is upgraded from URL-only entries to per-tracker state: `protocol` (http/https/udp/ws), `status` (working/not-working/waiting), `seeders` / `leechers`, `peers`, `lastAnnounceTime` / `nextAnnounceTime`, `error`
- The BT engine records per-URL results during each announce round; URLs not yet announced stay `waiting`

### Piece Tri-State Display & Persistence

- Task status responses gain `partialBitfield`: HTTP tasks report a partial-download piece bitmap (pieces with bytes landed > 0 but not complete), BT tasks always report an empty string; combined with `bitfield` the UI can render not-started / in-progress / completed tri-state maps
- Session serialization saves `btBitfield`, `httpNumPieces` / `httpPieceLen`; restoration rebuilds the bitmaps and backfills piece state from completed bytes — previously completed tasks lost their piece maps after restart

### Task File Verification (task.verifyFiles)

- New native RPC `task.verifyFiles`: runs existence checks, file size comparison, and streaming hash computation (`algorithm` accepts `size` / `sha256` / `sha1` / `md5` / `sha512`, case-insensitive)
- Path resolution and disk reads happen entirely in the engine: BT multi-file entries are joined as "name/relative-path", HTTP tasks use the actual on-disk path; unselected BT files are excluded
- Returns a structured result: `status` (`ok` / `missing` / `sizeMismatch`), `count`, `missing` / `mismatched` and `hashes` (`path` + hex `digest`, empty for `size` verification)
- `engine.getVersion`'s `features` adds `"verify-files"`; `xfer-storage` adds `file_digest_hex` sharing the underlying implementation with `verify_file_hash`

### UDP Hole Punching Standardization (ut_holepunch, libtorrent de-facto standard)

- Wire format aligned with libtorrent: `msg_type(1) + addr_type(1) + addr(4/16) + port(2)`, with only `failed` appending a 4-byte error code; message types and error codes exactly match `bt_peer_connection`. The previous private format could not be parsed by any standard client
- Relay (rendezvous) semantics aligned: resolve the target connection first (exact endpoint match + same-IP fallback), replying the appropriate `failed` when unreachable / unsupported / self-targeted; on success both sides receive a connect
- Initiator side added: once direct-dial retries are exhausted, the engine asks any connected peer advertising `ut_holepunch` to relay (capped at 2 rounds per target, 2 relays per round); previously it only ever answered relay requests, making double-NAT traversal with standard clients impossible
- Holepunch messages from peers that did not advertise `ut_holepunch` are ignored; PEX `added.f` flags corrected to libtorrent semantics (0x08 = holepunch-capable, 0x04 = uTP)

### Peer Info Extensions & Ban Display (task.getPeers)

- Each peer gains dial / transport stats fields: `downSpeed` / `upSpeed` / `tcpFails` / `utpFails` / `udpFails` / `attempting`
- New banned group: bans are recorded per IP (`addr` holds only the address, `port` empty) and report `remainingSecs` (0 = permanent), `source` and `banReason` (`manual` = banned by hand / `ban_list` = pushed via the ban list); one `getPeers` call returns all four groups — connected / attempting / disconnected / banned
- Ban entries distinguish their origin: `task.banPeer` is recorded as manual, `bt-ip-ban-list` pushes as list entries; persisted across sessions, with legacy session files defaulting to list entries

### BT Seed Duration (bt-seed-time)

- New global option `bt-seed-time` (minutes, 0 = unlimited): seeding stops automatically and the task turns complete once the duration is reached — a second seeding exit condition alongside `bt-seed-ratio`
- Changes are pushed hot to all active BT engines: seeding tasks re-evaluate against the new duration (the seeding start time is unchanged), and tasks finishing download later pick it up when they enter seeding; persisted with the session

### Wanted-Piece Bitmap (wantedBitfield)

- Task status responses gain `wantedBitfield` (aria2-compatible hex encoding, same format as `bitfield`): for BT tasks with a partial file selection it reports the pieces that must be downloaded; it is an empty string for full / no selection
- Pieces belonging only to unselected files are never set (the engine neither requests nor writes them), so the UI can tell "not selected, not needed" apart from "not downloaded" — previously a completed task still showed a few grey cells at the end that looked like missing pieces
- Pieces spanning a selected/unselected boundary still count as wanted (pieces cannot be split; the whole piece is downloaded while the unselected side is not written)

### Charset-Aware Text Decoding (xfer-types::text)

- New text decoding module: explicit charset → strict UTF-8 → GB18030 → lossy, used uniformly by magnet / .torrent / HTTP parsing
- Magnet `dn` percent-encoding, .torrent `name` and path segments, and HTTP `Content-Disposition: filename*` (RFC 5987, honoring declared gb2312/gbk charsets) and URL-path percent-encoding all go through this module

## Bug Fixes

- Fix misreported per-file progress on BT tasks: `files[].completedLength` used to be estimated as "file length × overall progress / total length of all files", so a file the user never selected still showed progress (observed as 80% / 11.4 MB). It is now accounted per piece boundary segment — each file only accumulates the bytes of completed pieces that fall inside it, unselected files stay at 0, and the per-file values sum up consistently with the task progress
- Fix task progress exceeding 100% (observed as 100.08%): with a partial file selection the total shrinks to the selected files' length while completed bytes were summed as whole piece lengths of wanted pieces, pulling the unselected side of boundary pieces into the numerator. Bytes are now attributed per segment, so `completedLength` never exceeds `totalLength` and a finished task reads exactly 100%
- Fix magnet/.torrent tasks skipping "awaiting file selection" and downloading directly: `task.add` previously passed through only `dir` / `out` / `checksum`, silently dropping task-level options such as `bt-file-selection` / `select-file`; all options except reserved protocol keys are now passed through, so the auto-pause-awaiting-selection flow works again
- Fix inaccurate task average speed: the formula `bytes/(ms/1000)` inflated the average when active time was under 2 seconds due to integer truncation (nearly 2x error at ms=1999, inflated early readings); it now computes `bytes*1000/ms`. Speed sampling also starts at the moment the task starts, so the first 1 Hz tick's bytes count toward the average (previously dropped, systematically underreporting short tasks)
- Fix the "awaiting file selection" state appearing late after magnet metadata is ready: the metadata-fetch loop's tracker announce (which can block up to a 15s timeout) and its 1-second polling were not cancellation-aware, deferring the pause intent until the round finished; cancellation is now handled first and the pause takes effect immediately
- Fix garbled Chinese task/file names (displayed as a run of `????`): Chinese sites commonly percent-encode the magnet `dn` in GBK, old Chinese torrents store `name` and path segments as GBK bytes (previously rejected outright as "info missing name"), and HTTP `filename*` headers declaring gb2312/gbk were decoded as UTF-8 — all now go through charset-aware decoding, see "Charset-Aware Text Decoding"
- Fix completed tasks losing piece progress after restart: piece bitmaps were not saved or restored across sessions, see "Piece Tri-State Display & Persistence"
- Fix saved file selections not taking effect and the detail view reopening with "none selected": `select-file` was previously only stored, never applied, see "File Selection (select-file) End-to-End"
- Fix HTTP piece bitmaps staying all-zero for files of just a few MB: the piece length was previously fixed to `min-split-size`, see "HTTP Piece Bitmap & Global Rate Limiting"
- Fix HTTP tasks falsely reporting `seeder=true` on completion: `seeder` now means "this endpoint is a BT task and its payload is complete"; it used to be computed as "completed ≥ total" for every task type, making the client mark normal downloads as "seeding"
- Fix speed-limit settings not taking effect: unit-suffixed values were rejected or silently treated as unlimited, and the HTTP download path had no rate-limit enforcement at all, see "HTTP Piece Bitmap & Global Rate Limiting"

## Behavior Changes

- Per-task rate limits now override the global one: a set task limit takes precedence and may be higher or lower than the global value; unset (0) follows global. Previously the effective value was min(per-task, global), so a task limit above the global one was clamped back
- Session restore is tightened: all non-terminal tasks (active / waiting / paused) are restored as paused and no longer auto-start; resume data is saved with the session, so a manual resume continues from the previous progress
- Unknown engine command-line options are no longer "warned and dropped": any `--key=value` is accepted as a global default, readable via `engine.getOptions` and persisted with the session; only bare positional arguments are treated as invalid

## Build & Release

- CI build matrix gains `linux-arm64` (aarch64-unknown-linux-musl static linking — no glibc dependency, no bundled lib/ directory), cross-compiled with cargo-zigbuild
- macOS engine-core artifacts are split per architecture: the TUI remains a universal dual-architecture binary, while the engine core ships as `xferrust-darwin-aarch64.tar.gz` and `xferrust-darwin-x86_64.tar.gz` — embedding clients fetch the matching artifact directly, no thin extraction needed
