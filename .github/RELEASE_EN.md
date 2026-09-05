**Summary**: A magnet-experience and BT-stability release — pick which files to download from a table after a magnet link is parsed (download starts only after confirmation), plus fixes for a peer session leak, rate-limiter deadlock and uTP stream corruption, hardening against path traversal and metadata poisoning, and completed ut_metadata serving, tracker reporting and choking behavior. The TUI is fully redesigned as well — the add-task dialog supports a per-task download directory (with a native system folder picker), magnet links start parsing immediately and expand the dialog in place into a file-selection table once metadata is ready, the main layout is rebuilt into a modern flat design, and the global info bar stays visible on task detail pages. Additionally, this release adds full BT seeding lifecycle management (configurable share-ratio auto-stop, continuous seeding until manual stop) and Android platform engine-core cross-compilation support.

## New Features

- Magnet file selection: the engine starts parsing metadata as soon as a magnet link is added; once parsed, the task pauses and a file table is shown (checkbox, file size, selection summary), and only the checked files are downloaded. Pieces spanning the selected/unselected boundary are downloaded whole, as BitTorrent requires
- Magnet tasks are resumable: a task still waiting for file selection reopens the file table after a restart instead of silently downloading everything
- Serve ut_metadata (BEP 9) requests: magnet peers get proper DATA/REJECT responses instead of being ignored
- `seed_duration` is now honored: seeding exits cleanly with a stopped announce when the configured time is up

### BT Seeding Lifecycle Management

- BT tasks can be configured to enter a seeding state after download completion (instead of finishing immediately), controlled by the global option `bt-seed-mode` (`true`/`false`)
- Seeding tasks display a live share ratio (uploaded / downloaded); a target share ratio `bt-seed-ratio` (e.g. `1.5`, `2.0`) can be set — seeding auto-stops when the ratio is reached
- Seeding tasks can be paused/resumed, and seeding can be manually stopped (RPC `task.stopSeed` / TUI `S` key)
- Continuous seeding until manual stop is supported (`bt-seed-ratio` of `0` disables auto-stop)
- TUI settings page adds seeding mode and target share ratio options; task list/detail views show upload speed and share ratio for seeding tasks

### Android Engine-Core Build

- Added Android (aarch64-linux-android / arm64-v8a) cross-compilation support — builds only the engine core (`xferrust`), no TUI
- `Cargo.toml` makes TUI dependencies (crossterm, ratatui, etc.) optional under a `tui` feature; `default = ["tui"]`; the `xfer` binary is marked `required-features = ["tui"]`
- CI adds a `build-android` job: cross-compiles with NDK r27c + API 24 (Android 7.0+); artifact `xferrust-android-arm64-v8a.tar.gz` is included in releases
- New local build script `scripts/build-android.sh`: auto-detects NDK host-tag (macOS / Linux), sets CC/CXX/AR/Linker env vars and invokes `cargo build --no-default-features --bin xferrust`

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

## UI Redesign

- Flat top bar: a single row with the brand logo on the left and ↓/↑ global speed plus active/waiting/stopped counters on the right, separated by a vertical bar
- Square task-area borders: top/bottom lines start aligned with the logo, end in rounded corners joined to the right vertical line, all four corners left open; the sidebar/content divider leaves headroom above and below
- Sidebar: removed the category heading, selection is now highlighted with a background (no more pointer)
- Task table: a separator line between the header and rows (same color as the border, same width as the columns, symmetric on both sides); header and rows keep one column of padding on each side, symmetric left/right
- Size column is now a compact "completed / total" format, the speed column is narrower, and task names no longer push later columns out of alignment

## Other Changes

- New file-selection API: `select_files(gid, file_indices)` / `get_selected_files(gid)`, applied live on running tasks; `add_uri` accepts a `bt-file-selection` option to pause a magnet task after parsing and wait for a selection
- Task status exposes a new `awaitingSelection` field, and `files[].selected` now reflects the real selection
- New seeding status `seeding` and `seedRatio` field; RPC adds `task.stopSeed` method; session save/restore handles seeding tasks correctly (serialized as `waiting` so they re-download on restart)
- Failed dial / session addresses are re-queued for a bounded number of retries instead of waiting for the next announce
- DHT hardening: known-peer table capped with FIFO eviction, inbound datagram processing concurrency limited
- Added 27 TUI rendering regression tests (border geometry, column alignment, dialog flow, detail top bar, etc.)
