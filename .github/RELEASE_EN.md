**Summary**

- New SFTP downloads: single files over `sftp://`, parallel channels and resumable transfers
- Password and OpenSSH private-key authentication; host keys are TOFU by default and a changed fingerprint is rejected
- Fixed UPnP port mapping, which had never actually been established
- Port mapping now supports both `WANIPConnection` and `WANPPPConnection` (PPPoE routers)
- Pausing a task saves the session immediately, so progress no longer rolls back after a restart
- Fixed a write failure after restarting when a task had just been added and the session stored an empty path
- LAN sends now explain an unreachable device in plain words and retry before giving up

## New Features

### SFTP downloads (`sftp://`)

- Single-file downloads from `sftp://[user[:password]@]host[:port]/path`; directories are rejected with a clear error instead of being walked.
- Several channels over one SSH connection read in parallel, with progress recorded per 1 MiB piece; after a pause or a dropped connection only the missing pieces are fetched.
- Authentication: a password in the URL, `sftp-password`, or an OpenSSH private key (`sftp-private-key`, with an optional `sftp-private-key-passphrase`).
- Host keys are TOFU by default: `~/.ssh/known_hosts` is trusted, first-seen keys are recorded in the engine's own `sftp_known_hosts`, and a changed fingerprint is rejected; `sftp-strict-host-key=true` requires the key to be recorded beforehand.
- `sftp-connections` sets the number of parallel channels (defaults to `split`, capped at 8) and `sftp-timeout` sets connect and request timeouts in seconds.
- Connection-level failures (drops, timeouts, short reads) are retried on the same address without re-downloading completed pieces; multiple addresses fail over to each other.
- `engine.getVersion.features` adds `sftp`.

### Command line

- `xferrust sftp://...` starts a download directly, like every other protocol.

## Bug Fixes

### Port mapping

- Fixed the internal address in UPnP mapping requests: it was sent as `0.0.0.0`, which standard routers reject (402), so no mapping had ever been created even on UPnP-capable networks.
- Fixed detection of the IGD service: only `WANIPConnection` was recognized, while PPPoE routers expose `WANPPPConnection` only and were reported as unsupported.
- Mapping failures are no longer silent: they are logged as warnings together with the router's error code and description.

### Tasks and sessions

- Pausing a task now saves the session immediately: quitting after a pause no longer rolls the downloaded size back to an older value on restart.
- Fixed a write failure on restart when a task had just been added and the session stored an empty path as the target.

### LAN transfer

- When a send cannot reach the other device, the task now says the device is offline or on a different network instead of showing a bare `No route to host`.
- Before declaring the device offline it retries at the discovery interval, so a device that just changed networks or woke from sleep is not failed on the first attempt.

## Behavior Changes

- On UPnP-capable routers the BT listen port now really gets both TCP and UDP mappings (previously none were created).
- On networks without NAT-PMP, mapping and unmapping are noticeably faster because candidate gateways are no longer probed one by one.
- `engine.getVersion.features` now includes `sftp`, taking the feature count from 17 to 18.
- All other command-line arguments and RPC semantics are unchanged.
