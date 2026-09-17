**Summary**

- Fixed BT downloads being far slower than other clients: IPv6 peers returned by the tracker used to be dropped entirely and DHT dual-stack always fell back to IPv4-only on macOS — a public torrent measured a peak of 17.1 MB/s, up from <1 MB/s
- Tracker announces now fully support IPv6: bare IPv6 literals in non-compact peer lists are parsed, and the BEP 32 `peers6` field is accepted
- Every HTTP tracker is now announced to over both IPv4 and IPv6 concurrently with the results merged and deduplicated, so seeders that trackers only return to IPv6 sources are no longer missed
- Fixed DHT dual-stack binding always failing on macOS (the `[::]` bind address was treated as a hostname) and silently falling back to IPv4-only
- Fixed every DHT query timing out: responses were parsed as queries, failed, and silently discarded — they are now dispatched to the waiting query by transaction id
- DHT query timeout 10s → 3s, and find_node for all bootstrap nodes plus one iteration now runs concurrently for a faster cold start

## Bug Fixes

- Fixed IPv6 addresses in non-compact peer lists being dropped: the address and port were concatenated into `ip:port` and re-parsed, which always fails for bare IPv6 literals (the port gets swallowed into the address) and discarded the whole peer; peers are now parsed from the IP literal with the port assembled afterwards, so a single invalid entry only skips that entry
- Fixed the tracker `peers6` field being ignored: added BEP 32 compact parsing (18 bytes per entry = 16-byte IPv6 + 2-byte port, port 0 dropped), with a ragged tail truncated and warned about
- Fixed never obtaining IPv6 seeders: hostnames resolve to IPv4 first, so announces were always sent over IPv4 while trackers only return IPv6 peers to IPv6 sources (BEP 7); every tracker is now announced to over both IPv4 and IPv6 concurrently with the results merged and deduplicated — skipped automatically when the host has no IPv6 address or is a bare IP, and the IPv6 result is used as a fallback when the IPv4 announce fails
- Fixed DHT dual-stack nodes always failing to start on macOS: the `[::]` bind address was handed to the resolver as a hostname (and then fell back to IPv4-only); it is now parsed as an IP literal (brackets stripped)
- Fixed address-family mismatch when sending from / receiving on a dual-stack DHT socket: IPv4 destinations are converted to v4-mapped before sending and v4-mapped sources are normalized back to IPv4, keeping the routing table, known_peers and peer addresses handed to BT as IPv4
- Fixed every DHT KRPC query timing out: callers raced the resident receive loop for the same socket, and responses picked up by the loop were parsed as queries, failed, and silently discarded, leaving callers to time out; pending queries are now registered as "transaction id → channel" and the receive loop dispatches responses by tid (the entry is removed on send failure or timeout)
- Fixed slow DHT cold start: find_node queries for all bootstrap nodes and one iteration now run concurrently

## Behavior Changes

- DHT KRPC query timeout tightened from 10s to 3s: unreachable nodes fail faster and bootstrapping / get_peers iterations are no longer stalled by slow nodes
- Each HTTP tracker now receives two announce requests per round (IPv4 + IPv6): tracker-side request volume doubles in exchange for all seeders that are only returned to IPv6 sources; no extra request is made when IPv6 is unavailable

## Build & Release

- Engine version bumped to 0.3.1 (`engine.getVersion` reports 0.3.1)