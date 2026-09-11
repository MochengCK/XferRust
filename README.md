<p align="center">
  English | <a href="./README-CN.md">简体中文</a>
</p>

# XferRust

A high-performance, lightweight standalone download engine written in Rust,
supporting **HTTP(S)** and **BitTorrent (BT)** downloads. It ships with a
command-line interface and can also run as a background service for integration
with web apps, desktop applications, and other clients.

## What It Can Do

- **Download HTTP / HTTPS files** with resume support — pause and continue
  without wasting what has already been downloaded.
- **Download BitTorrent torrents and magnet links** with multi-peer parallel
  acceleration and rarest-first piece selection.
- **Selective file downloading for magnets**: paste a magnet link and the engine
  parses metadata immediately; once parsed, a file table pops up
  (checkboxes + file sizes + selection summary) and the download starts only
  after you confirm — no need to download an entire torrent for a few files.
- **Dual-track adaptive scheduling**: HTTP connection counts (`split`) and BT
  peer counts (`bt-max-peers`) are scheduled independently, each growing or
  shrinking adaptively based on marginal throughput gains — expand when it
  helps, rotate out slow peers when stalled.
- **Automatic integrity verification** after completion
  (sha-1 / sha-256 / sha-512 / md5).
- **Visual terminal UI** with live progress, speed, ETA, and speed trend.
- **Run as a background daemon** and be controlled remotely over RPC by other
  programs.
- **Resource friendly**: low memory and CPU usage, fast cold start.

## Installation

### Prebuilt Binaries (Recommended)

Download one standalone archive per platform as needed:

| Platform | TUI Edition (interactive UI + downloads) | Engine Core Edition (no TUI, background service / embedding) |
|---|---|---|
| Linux x86_64 | `xfer-tui-linux-x86_64.tar.gz` | `xferrust-linux-x86_64.tar.gz` |
| Linux arm64 | `xfer-tui-linux-arm64.tar.gz` | `xferrust-linux-arm64.tar.gz` (musl static) |
| Windows x86_64 | `xfer-tui-windows-x86_64.tar.gz` | `xferrust-windows-x86_64.tar.gz` |
| macOS (TUI is a universal Intel + Apple Silicon binary) | `xfer-tui-darwin-universal.tar.gz` | `xferrust-darwin-aarch64.tar.gz` / `xferrust-darwin-x86_64.tar.gz` (per-architecture) |
| Android arm64-v8a | — | `xferrust-android-arm64-v8a.tar.gz` |

Download from the GitHub [Releases](https://github.com/MochengCK/XferRust/releases) page.

### Build from Source

Requires the [Rust toolchain](https://rustup.rs/):

```bash
cargo build --release
# Artifacts:
#   target/release/xfer      command-line UI (TUI edition)
#   target/release/xferrust  engine daemon (no TUI, for app integration)
```

Check the version: `xfer --version`

## Quick Start

**Download a file directly:**

```bash
xfer download https://example.com/bigfile.zip -d ~/Downloads -o bigfile.zip
```

**Open the visual main interface:**

```bash
xfer
```

**Background service + remote control:**

```bash
xfer daemon --rpc-secret=mytoken --dir=~/Downloads &
xfer add https://example.com/a.zip --token mytoken
```

---

## Command-Line Guide

`xfer` covers three usage styles: the **visual main interface**,
**single-task downloads**, and the **daemon + remote subcommands**.

### 1. Visual Main Interface (Recommended Entry)

```bash
xfer                    # equivalent to xfer tui
xfer tui [-d dir] [-j max-concurrent]
```

| Option | Default | Description |
|---|---|---|
| `-d, --dir` | Current directory | Download directory |
| `-j, --max-concurrent` | `3` | Max concurrent downloads |

The main interface embeds the engine (no daemon required) and refreshes in
real time. Tasks and settings persist in the session file
`~/.xfer/session.json` (auto-saved every 30 seconds and on exit) and are
restored on restart; when `-d` / `-j` are not given explicitly, the values
saved in the session are reused.

**List View**

```
┌ XferRust v0.2.0 ──────────────────────────────┐
│ Total 9.6 MiB/s   Active 2  Waiting 1  Done 1 │
├─ Tasks (4)───────────────────────────────────┤
│ active   ████████░░░░ 66.7% ubuntu.iso  ...   │ ← selected row highlighted
│ waiting  ░░░░░░░░░░░░  0.0% backup.tar.zst …  │
├───────────────────────────────────────────────┤
│ (action feedback message, shown for 2s)       │
└───────────────────────────────────────────────┘
 a add · Enter details · ↑↓ select · r pause/resume · x remove · c clear done · s settings · q quit
```

| Key | Action |
|---|---|
| `a` | Open the input box; paste a URL and press `Enter` to add a task, `ESC` to cancel (magnet links enter the metadata & file-selection flow) |
| `↑` `↓` or `k` `j` | Move task selection |
| `Enter` | Open task details (progress gauge + speed sparkline) |
| `r` | Pause / resume the selected task (toggle) |
| `x` | Remove the selected task (confirm to delete; optionally delete downloaded files) |
| `c` | Clear all completed records |
| `s` | Open the settings page |
| `Tab` | Toggle focus between the task list and the category sidebar |
| `1` ~ `5` | Quick-switch category filters |
| `q` / `Ctrl-C` | Quit |

**Magnet link flow** (paste a `magnet:` link via `a`): a parsing popup appears
first (torrent name, connected peers, elapsed time); once metadata is parsed
the task auto-pauses and a file table pops up — `↑` `↓` / `PgUp` `PgDn` to
move, `Space` to check, `a` to select all / invert, `Enter` to confirm
(download only the checked files), `Esc` to cancel and remove the task; the
bottom bar shows live "n/N selected · selected size / total size". Tasks
exited without confirmation re-open the table on the next launch.

**Detail View**: a gauge (percentage, downloaded/total, speed, ETA, average
speed) + a sparkline of the last 60 seconds of speed. `ESC` / `Enter` returns
to the list; `r` / `x` behave as in the list; `Tab` switches focus between the
tracker and peer tables, arrow keys and `PgUp` / `PgDn` scroll, and `t` adds a
tracker to a BT task.

**Settings Page** (`s` key): adjust max concurrency, HTTP split connections,
max connections per server, `bt-max-peers`, `min-split-size`,
download/upload rate limits, the default download directory, manage the global
tracker list and tracker subscriptions, and the UI language
(Simplified / Traditional / English, persisted to the session).

> The UI language can also be set at launch via an environment variable:
> `XFER_LANG=zh|en|zh_tw xfer`.

### 2. Single-Task Download

```bash
xfer download <url> [-d dir] [-o filename] [--checksum algo=digest]
xfer <url> ...          # a bare URL is equivalent to download
```

| Option | Description |
|---|---|
| `-d, --dir <dir>` | Save directory; defaults to the current directory |
| `-o, --out <filename>` | Output file name (priority: out > Content-Disposition > URL) |
| `--checksum algo=digest` | Verify after completion; supports `sha-1` / `sha-256` / `sha-512` / `md5` |

Examples:

```bash
xfer download https://example.com/bigfile.zip -d ~/Downloads -o bigfile.zip
xfer download https://example.com/iso --checksum sha-256=ab34…
```

Behavior: a full-screen TUI shows live progress; `q` / `ESC` / `Ctrl-C`
cancels (exit code 130); on success exit code 0 and the file path is printed;
on failure exit code 1 with the error code and message.

### 3. Daemon

```bash
xfer daemon [--rpc-listen-port=port] [--rpc-secret=secret]
            [--dir=dir] [--max-concurrent-downloads=N]
```

> `--log` / `--log-level` only apply to `xferrust` (see the table below);
> `xfer daemon` accepts but does not enable them yet.

| Option | Default | Description |
|---|---|---|
| `--rpc-listen-port` | `6800` | RPC listen port (127.0.0.1) |
| `--rpc-secret` | none | Auth secret; no auth when unset |
| `--dir` | `.` | Default download directory |
| `--max-concurrent-downloads` | `5` | Max concurrent downloads |
| `--log` / `--log-level` | none | File log / level (error/warn/notice/info/debug); only effective for `xferrust` — logs rotate by size (10 MB per file, 5 kept) |

The `xferrust` binary takes the same arguments as `xfer daemon`, for bundling
with applications. Beyond the core options above, any `--key=value` option is
accepted as an **externally supplied default**: implemented keys take effect at
startup, while keys not yet implemented are stored in the global options store
(readable via `engine.getOptions`) without errors or warnings — host
applications can safely pass their entire configuration through.

The daemon and the TUI share the session file `~/.xfer/session.json`: history
tasks and settings are restored at startup (only explicit `--dir` /
`--max-concurrent-downloads` override session settings), auto-saved while
running, and written on exit.

### 4. Remote Subcommands (Control a Running Daemon)

Common options: `--connect <ws-url>` (default `ws://127.0.0.1:6800/jsonrpc`)
and `--token <secret>` (matching the daemon's `--rpc-secret`).

```bash
# Add a task; returns a gid
xfer add <url> [-d dir] [-o filename] [--checksum algo=digest] [--token secret]

# Add a BT task (.torrent file)
xfer add <file.torrent> [-d dir] [--token secret]

# Add a magnet link task (the engine fetches metadata via ut_metadata and downloads)
xfer add "magnet:?xt=urn:btih:<40-hex>&dn=name&tr=http://tracker/announce" [--token secret]

# Task details (JSON)
xfer tell <gid>

# Task list (--scope all|active|waiting|stopped, default all)
xfer list [--scope active]

# Task operations
xfer pause <gid>
xfer resume <gid>
xfer remove <gid>

# Global stats (total speed + per-status task counts)
xfer stat
```

A complete session example:

```bash
$ xfer daemon --rpc-secret=tok --dir=~/Downloads &
RPC listening on http://127.0.0.1:6800/jsonrpc (Ctrl-C to exit)

$ xfer add https://example.com/big.zip --token tok
9f3ba2c4d81e0755

$ xfer list --token tok
GID              Status     Progress    Size      Speed      File
9f3ba2c4d81e0755 active     42.3%   2.2 GiB  8.4 MiB/s  big.zip

$ xfer pause 9f3ba2c4d81e0755 --token tok
OK

$ xfer stat --token tok
Download 0 B/s · active 0 · waiting 0 · stopped 0 (total 0)
```

### 5. Exit Codes

| Code | Meaning |
|---|---|
| `0` | Success (download completed / normal exit) |
| `1` | Task failure (network error, checksum mismatch, etc.) or RPC failure |
| `2` | Usage error (missing arguments, unknown subcommand) |
| `130` | Cancelled by user (q / ESC / Ctrl-C) |

---

## External Client Integration Guide

For client developers integrating XferRust download capabilities into their
apps: RPC connection, authentication, method calls, event subscription, and
the aria2-compatible frontend protocol.

### 1. Deployment

The engine runs as a daemon, controlled by clients over local RPC:

```bash
# Start the daemon (defaults to 127.0.0.1:6800, localhost only)
xfer daemon --rpc-listen-port=6800 --rpc-secret=mytoken --dir=~/Downloads

# Or use the daemon binary bundled with your app (same args, plus --log/--log-level)
xferrust --rpc-listen-port=6800 --rpc-secret=mytoken
```

| Option | Default | Description |
|---|---|---|
| `--rpc-listen-port` | `6800` | RPC listen port (binds 127.0.0.1) |
| `--rpc-secret` | none | RPC auth secret; no auth when unset |
| `--dir` | `.` | Default download directory |
| `--max-concurrent-downloads` | `5` | Max concurrent downloads |
| `--log` / `--log-level` | none | File log and level (error/warn/notice/info/debug); only effective for `xferrust` (size-based rotation, 10 MB per file, 5 kept) |

Any other `--key=value` is accepted as an externally supplied default option
(see "Command-Line Guide → Daemon").

### 2. Connection & Protocol

**Endpoint**: `POST /jsonrpc` (single requests) and `WS /jsonrpc` (persistent
connections) share the same address, by default
`http://127.0.0.1:6800/jsonrpc`.

**Framing**: JSON-RPC 2.0, with batch support (an array request yields an
array response; only entries carrying an `id` are answered).

```json
// Request
{"jsonrpc": "2.0", "id": 1, "method": "engine.getVersion", "params": {"token": "mytoken"}}
// Success response
{"jsonrpc": "2.0", "id": 1, "result": {"name": "XferRust", "version": "0.2.0", "features": ["http", "resume", "checksum", "bt", "events"]}}
// Error response
{"jsonrpc": "2.0", "id": 1, "error": {"code": 1, "message": "Unauthorized"}}
```

**Dual protocol families, auto-detected**: if the first request on a
connection hits `task.*` / `engine.*` / `events.*` it is treated as the native
protocol; if it hits `aria2.*`, a legacy unprefixed name, or `system.*` it is
treated as the frontend-compatible protocol. A connection is pinned to one
protocol family, and event frames are filtered per family.

### 3. Authentication

- **Native protocol**: the `"token"` field in the params object must strictly
  equal `--rpc-secret`.
- **Frontend-compatible protocol**: the first positional parameter
  `"token:<secret>"` (with the `token:` prefix); the server strips it before
  dispatching.
- When no secret is configured, authentication is skipped; failed auth returns
  `{"error": {"code": 1, "message": "Unauthorized"}}`.

### 4. Native Protocol Methods

Numeric fields are real JSON numbers.

#### 4.1 Task Management

| Method | Params | Returns |
|---|---|---|
| `task.add` | `uris`(array, required), `dir`, `out`, `checksum`, `position`(optional, ≥0 inserts into queue); or `torrent`(base64); or `magnet`(magnet link) | `{"gid": "<16-hex>"}` |
| `task.tell` | `gid`, `keys`(array, optional) | Task status object |
| `task.list` | `scope`("active"/"waiting"/"stopped"/"all", default "all"), `offset`, `num`(-1 = all), `keys` | Array of task status objects |
| `task.pause` | `gid` | `{"ok": true}` |
| `task.resume` | `gid` | `{"ok": true}` |
| `task.remove` | `gid` | `{"ok": true}` |
| `task.purgeResults` | — | `{"ok": true}` |
| `task.removeResult` | `gid` (must be terminal) | `{"ok": true}` |
| `task.getFiles` | `gid` | File list |
| `task.getUris` | `gid` | URI list |
| `task.getPeers` | `gid` | Peer list (BT) |
| `task.getTrackers` | `gid` | Tracker list (BT) |
| `task.addTrackers` | `gid`, `trackers`(non-empty array) | `{"ok": true}` |
| `task.getOption` | `gid` | Options object (global defaults + per-task overrides) |
| `task.changeOption` | `gid`, key-value pairs | `{"ok": true}` |

Add-task example:

```json
{"jsonrpc": "2.0", "id": 1, "method": "task.add",
 "params": {"token": "mytoken", "uris": ["https://example.com/big.zip"],
            "dir": "/Downloads", "out": "big.zip",
            "checksum": "sha-256=<hex>"}}
```

Supported `checksum` algorithms: `sha-1` / `sha-256` / `sha-512` / `md5`.

#### 4.2 Engine Management

| Method | Params | Returns |
|---|---|---|
| `engine.getVersion` | — | `{"name", "version", "features"}` |
| `engine.globalStat` | — | `{"downloadSpeed", "uploadSpeed", "numActive", "numWaiting", "numStopped", "numStoppedTotal"}` |
| `engine.getOptions` | — | Global options object |
| `engine.changeOptions` | key-value pairs (applied: `max-concurrent-downloads`, `dir`, `split`, `max-connection-per-server`, `min-split-size`, `bt-max-peers`, `bt-adaptive`, `max-overall-download-limit`, `max-overall-upload-limit`, `bt-encryption`, `bt-protocol`; others ignored) | `{"ok": true}` |
| `engine.saveSession` | — | `{"ok": true}` |
| `engine.shutdown` / `engine.forceShutdown` | — | `{"ok": true}` |

Global trackers and subscriptions:

| Method | Params | Returns |
|---|---|---|
| `engine.getTrackers` | — | `{"trackers": [URL, ...]}` |
| `engine.addTracker` | `tracker`(URL) | `{"ok": true}` |
| `engine.removeTracker` | `tracker`(URL) | `{"ok": true}` |
| `engine.getSubscriptions` | — | Subscription list |
| `engine.addSubscription` | `name`, `url`, `enabled`(optional, default true) | Subscription object |
| `engine.removeSubscription` | `id` | `{"ok": true}` |
| `engine.toggleSubscription` | `id` | `{"ok": true}` |
| `engine.refreshSubscription` | `id` | `{"count": n}` (fetched entries) |
| `engine.refreshAllSubscriptions` | — | `{"count": n}` |
| `engine.getAutoUpdateTrackers` | — | `{"enabled": bool}` |
| `engine.setAutoUpdateTrackers` | `enabled`(bool) | `{"ok": true}` |

Subscription behavior:

- **Fetch on add/enable**: `addSubscription` and `toggleSubscription`
  (re-enabling) immediately fetch once in the background and sync into the
  global tracker list; TUI and RPC clients behave identically, so callers
  never need a follow-up refresh.
- **Sync semantics (not add-only)**: when a subscription refreshes, trackers
  newly offered remotely are added to the global list; trackers removed
  remotely and previously contributed by that subscription are pruned.
  Manually added ones (`engine.addTracker`) and those still provided by other
  subscriptions are unaffected.
- **Daily auto-update**: a background job checks hourly and refreshes only
  subscriptions not updated for ≥24h (when `autoUpdateTrackers` is on).
  Manual `refreshSubscription` / `refreshAllSubscriptions` ignore the TTL and
  refresh immediately in full.
- **Safety valve**: an empty remote list is treated as an anomaly — existing
  trackers are kept and an error is recorded, avoiding accidental wipeouts.

#### 4.3 Task Status Object

```
gid, status, totalLength, completedLength, uploadLength, downloadSpeed,
uploadSpeed, bitfield, connections, errorCode, errorMessage, elapsedMs, dir,
files[{index, path, length, completedLength, selected, uris[{uri, status}]}],
numSeeders, seeder, numPieces, pieceLength
```

- `status`: `waiting` / `active` / `paused` / `complete` / `error` / `removed`
- `errorCode`: `0` none, `2` timeout, `3` not found, `5` network, `9` checksum mismatch, `1` other

### 5. Event Subscription

Native-protocol clients send `events.subscribe` once on the WebSocket
connection; the server then pushes continuously:

| Engine event | Method | params |
|---|---|---|
| Task started | `task.start` | `{"gid"}` |
| Paused | `task.pause` | `{"gid"}` |
| Stopped | `task.stop` | `{"gid"}` |
| Download completed | `task.complete` | `{"gid"}` |
| Error | `task.error` | `{"gid", "errorCode", "errorMessage"}` |
| Progress (1 Hz) | `task.progress` | `{"gid", "status", "completedLength", "totalLength", "downloadSpeed"}` |

Event frames carry no `id` field, which distinguishes them from responses:

```json
{"jsonrpc": "2.0", "method": "task.progress",
 "params": {"gid": "abc123...", "status": "active",
            "completedLength": 1048576, "totalLength": 10485760, "downloadSpeed": 262144}}
```

Recommended pattern: `events.subscribe` + `task.progress` event-driven UI
refresh, no polling; reconnect and re-subscribe after disconnection.

### 6. Frontend-Compatible Protocol (aria2 Style)

For existing aria2-based clients: positional parameters, numeric fields
carried as strings.

- Business methods: `aria2.addUri` / `aria2.addTorrent` / `getPeers` / `remove` / `forceRemove` /
  `pause` / `forcePause` / `unpause` / `tellStatus` / `tellActive` / `tellWaiting` /
  `tellStopped` / `getGlobalStat` / `getVersion` / `getFiles` / `getURIs` / `getOption` /
  `changeOption` / `getGlobalOption` / `changeGlobalOption` / `purgeDownloadResult` /
  `removeDownloadResult` / `saveSession` / `shutdown` / `forceShutdown`
- System methods: `system.multicall` / `system.listMethods` / `system.listNotifications`
- Events (no subscription needed; pushed automatically once the protocol family is detected): `aria2.onDownloadStart` /
  `onDownloadPause` / `onDownloadStop` / `onDownloadComplete` / `onDownloadError` /
  `onBtDownloadComplete`

Call example:

```json
{"jsonrpc": "2.0", "id": 1, "method": "aria2.addUri",
 "params": ["token:mytoken", ["https://example.com/a.zip"], {"dir": "/Downloads"}]}
```

### 7. In-Process Rust Integration

Clients can also embed the engine directly as a library without RPC
(`xfer-engine` has no transport-layer dependencies):

```rust
use serde_json::json;
use xfer_engine::TaskManager;

let mgr = TaskManager::start(std::path::PathBuf::from("/Downloads"), 3);
let gid = mgr.add_uri(
    vec!["https://example.com/big.zip".into()],
    &json!({}),
    None,
)?;
let mut events = mgr.events().subscribe(); // broadcast event stream
// mgr.tell_status_native / pause / unpause / remove / list_native / global_stat_native
```

Magnet tasks support per-file downloads: pass the `bt-file-selection` option
at add time; after metadata is parsed the task auto-pauses
(status `awaitingSelection = true`); read the file list and sizes from
`files[]` for the user to choose, then pass 0-based file indexes and resume:

```rust
let gid = mgr.add_uri(
    vec!["magnet:?xt=urn:btih:...".into()],
    &json!({"bt-file-selection": "true"}),
    None,
)?;
// After parsing (task paused): files[].index - 1 is the file index
mgr.select_files(&gid, &[0, 2])?; // download only the 1st and 3rd files
mgr.unpause(&gid)?;               // start downloading immediately after confirmation
```

### 8. Integration Checklist

1. Start or connect to the daemon; handshake with `engine.getVersion` to
   confirm the version and capabilities.
2. When a secret is configured, carry the token on every request (native
   `params.token` / compatible `token:<secret>`).
3. On the WebSocket connection, send `events.subscribe` first and dispatch
   event frames by `method`.
4. Maintain a task table keyed by `gid`; update progress from
   `task.progress` and settle tasks on terminal events (`complete` / `error`).
5. After a reconnect, re-subscribe and reconcile with a full `task.list`.

---

## License

GPL-3.0-only (GNU General Public License v3.0, this version only). See
[LICENSE](LICENSE) for the full license text.
