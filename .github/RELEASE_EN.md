**Summary**

- Added LAN file transfer: any two devices on the same network (computer to computer, computer to phone, and so on) discover each other and send / receive files, with a 6-digit short authentication code shown on both ends to prevent man-in-the-middle attacks
- Transfers need no internet account or third-party server; transfer records and settings (master switch / device name / receive directory) persist across restarts
- Adding a BT task now verifies existing data on disk piece by piece and inherits only what is actually usable, so partial downloads no longer start from zero
- New `task.previewBt`: inspect whether each file exists, how many bytes are checked / verified, and live progress before adding the task
- Verification is now parallel and uses SHA-NI when available, with live per-file progress; large and multi-file torrents verify faster with unchanged accuracy
- BT tasks preallocate disk space before going online and fail immediately when space is insufficient, instead of running out of space mid-download
- Faster cold start: tracker / DHT / IPv6 announces run in parallel, peers are dialed as soon as they arrive, and candidates are both more numerous and quicker to connect
- Fixed magnet / torrent sessions dropping real clients such as qBittorrent right after the handshake (Fast Extension first-message ordering)

## New Features

### LAN file transfer (`lan.*`)

- Devices on the same LAN are discovered automatically and files are sent / received peer to peer, with no internet service or third-party account; device type does not matter — computer to computer and computer to phone both work.
- Both ends display the same 6-digit short authentication code; the user compares them before the transfer to prevent man-in-the-middle attacks.
- The transfer task table includes direction, file name, size, transferred bytes, status, peer device, and creation / finish times; records survive restarts, and unfinished records are closed as "interrupted".
- Active transfers can be cancelled and records removed; completed incoming records can delete their on-disk file at the same time, while outgoing records never delete the source file.
- Incoming file names are stripped of paths and control characters; existing files in the destination are never overwritten — a unique name is chosen instead.
- New `lan.peers` / `lan.tasks` / `lan.send` / `lan.cancel` / `lan.remove` / `lan.config` / `lan.setConfig`; `engine.getVersion.features` adds `lan` and `lan-config`.
- Transfer settings (master switch / local device name / receive directory) persist and apply immediately; turning the master switch off interrupts active transfers.

### Existing-data verification and resume (`task.previewBt`, `verifiedLength`)

- When adding a BT task, existing same-name / partial files in the destination are verified piece by piece with SHA-1, and only pieces actually present on disk and passing the hash are inherited; files with the right length but corrupted content are no longer mistaken for complete.
- New `task.previewBt`: verification runs in the background and clients poll for results; it returns `done` / `verifying`, checked / verified bytes, and piece counts.
- Preview and task status both include per-file data: `exists` / `checkedLength` / `verifiedLength` in `files[]`; missing or corrupted files do not advance progress.
- Task status adds `verifying` and `verifyProgress`, with progress consistently measured as verified bytes / total bytes — the same basis used by subsequent download progress.
- Verification runs in parallel over contiguous piece ranges (up to 8 workers), opens files on demand with a bounded handle cache, enables SHA-NI on supported CPUs, and falls back to a software implementation elsewhere; results remain per-piece SHA-1.
- When a saved `.torrent` with the same info_hash exists, magnet tasks reuse its file layout before verification and resume.

### BT disk preallocation

- New and resumed BT tasks preallocate every file to its final length before going online; insufficient space fails the task at startup instead of mid-download.
- Existing-data detection happens before preallocation, so freshly allocated empty files are not mistaken for existing data and inherited content is not truncated.
- Preallocation happens after the resume bitfield is restored and before the network starts, so verification and resume see exactly the same content as before.

### BT cold start and peer discovery

- Tracker announces, DHT get_peers, and IPv6 supplemental announces no longer serialize startup; whichever path returns peers first feeds the pool and triggers dialing immediately.
- Peers from each tracker are connected as they arrive instead of waiting for every tracker to time out; the first 10 seconds allow 3x the connection cap, then the engine returns to `bt-max-peers`.
- The candidate request per announce is raised to `bt-max-peers` (50–200); unreachable candidates do not occupy connection slots indefinitely.
- IPv6 supplemental announces are skipped when there is no public IPv6 route; when IPv6 is available, the tracker client is cached instead of rebuilt every round.
- DHT lookups start immediately during cold start and retry after 2 / 5 seconds, without stacking duplicate in-flight queries.

## Bug Fixes

- Fixed magnet / torrent sessions dropping real clients such as qBittorrent immediately after the handshake: Fast Extension requires the first message after the handshake to be HaveAll / HaveNone / Bitfield, but the extension handshake was sent first and treated as a protocol violation; messages now follow BEP 6 ordering, with HaveNone sent for magnets before metadata is available.
- Fixed DHT Port being sent to Fast Extension peers where message id 9 means HaveAll and could be treated as a violation; DHT Port is no longer sent to peers that support Fast Extension.
- Fixed DHT finding no peers on networks without public IPv6: DHT over IPv6 was enabled by default, causing dual-stack bootstrap / get_peers to time out and stall IPv4 discovery; it is now off by default and IPv6 trackers are only tried when a usable IPv6 route exists.
- Fixed under-reading when verifying existing data for multi-file / large torrents: opening every torrent file at once hit the process file-handle limit and later files were treated as missing (manifesting as "many bytes downloaded but only a fraction verified"); files are now opened on demand with a bounded handle cache.
- Fixed verification progress overshooting and then falling back: progress used to be based on processed bytes, so corrupt or missing pieces pushed it to 100% before dropping to the real value; it is now based on verified bytes.
- Fixed LAN transfer exposing the host by default: the master switch now defaults to off and must be explicitly enabled.

## Behavior Changes

- LAN transfer defaults to off, so upgrading does not automatically accept files from the network; existing configuration keeps its current value.
- DHT over IPv6 defaults to off; enable it explicitly when the network has public IPv6 and IPv6 DHT is desired.
- BT tasks with existing data verify on disk before going online, adding a verification phase at startup that is more noticeable for larger files.
- Preallocation reserves the full final size before downloading, so observable free space drops earlier, while preallocated empty files are not counted as downloaded content.
- During verification the task status is `verifying` and progress advances by verified bytes; clients should present `verifying` separately from ordinary download progress.
