**Summary**

- Fixed BT downloads being far slower than other clients: IPv6 peers returned by the tracker used to be dropped entirely and DHT dual-stack always fell back to IPv4-only on macOS — a public torrent measured a peak of 17.1 MB/s, up from <1 MB/s
- Tracker announces now fully support IPv6: bare IPv6 literals in non-compact peer lists are parsed, and the BEP 32 `peers6` field is accepted
- Every HTTP tracker is now announced to over both IPv4 and IPv6 concurrently with the results merged and deduplicated, so seeders that trackers only return to IPv6 sources are no longer missed
- Fixed DHT dual-stack binding always failing on macOS (the `[::]` bind address was treated as a hostname) and silently falling back to IPv4-only
- Fixed every DHT query timing out: responses were parsed as queries, failed, and silently discarded — they are now dispatched to the waiting query by transaction id
- DHT query timeout 10s → 3s, and find_node for all bootstrap nodes plus one iteration now runs concurrently for a faster cold start
- Fixed uTP / UDP tracker / BT listening being IPv4-only: uTP and IPv6 inbound connections could not start on IPv6 hosts, and sending from a dual-stack socket to an IPv6 peer failed with `EINVAL` (IPv6 peers just waited out the handshake timeout and fell back to TCP)
- HTTPS now also trusts the local system root store (corporate proxies, private CAs and OS certificate stores no longer fail across the board) and `no-proxy` really takes effect

## Bug Fixes

- Fixed IPv6 addresses in non-compact peer lists being dropped: the address and port were concatenated into `ip:port` and re-parsed, which always fails for bare IPv6 literals (the port gets swallowed into the address) and discarded the whole peer; peers are now parsed from the IP literal with the port assembled afterwards, so a single invalid entry only skips that entry
- Fixed the tracker `peers6` field being ignored: added BEP 32 compact parsing (18 bytes per entry = 16-byte IPv6 + 2-byte port, port 0 dropped), with a ragged tail truncated and warned about
- Fixed never obtaining IPv6 seeders: hostnames resolve to IPv4 first, so announces were always sent over IPv4 while trackers only return IPv6 peers to IPv6 sources (BEP 7); every tracker is now announced to over both IPv4 and IPv6 concurrently with the results merged and deduplicated — skipped automatically when the host has no IPv6 address or is a bare IP, and the IPv6 result is used as a fallback when the IPv4 announce fails
- Fixed DHT dual-stack nodes always failing to start on macOS: the `[::]` bind address was handed to the resolver as a hostname (and then fell back to IPv4-only); it is now parsed as an IP literal (brackets stripped)
- Fixed address-family mismatch when sending from / receiving on a dual-stack DHT socket: IPv4 destinations are converted to v4-mapped before sending and v4-mapped sources are normalized back to IPv4, keeping the routing table, known_peers and peer addresses handed to BT as IPv4
- Fixed every DHT KRPC query timing out: callers raced the resident receive loop for the same socket, and responses picked up by the loop were parsed as queries, failed, and silently discarded, leaving callers to time out; pending queries are now registered as "transaction id → channel" and the receive loop dispatches responses by tid (the entry is removed on send failure or timeout)
- Fixed slow DHT cold start: find_node queries for all bootstrap nodes and one iteration now run concurrently
- Fixed uTP being unable to talk to IPv6 peers: the uTP socket was bound to IPv4 only, and on a dual-stack listener every send to an IPv6 / IPv4 peer needs an address-family conversion — without it sendto fails with `Invalid argument (os error 22)` and the uTP dial to an IPv6 peer can never succeed (each peer wastes a full handshake timeout, measured at 3s per peer, before falling back to TCP); the socket is now bound dual-stack to `[::]` with v4-mapped conversion on both send and receive
- Fixed BT being unable to listen on IPv6-only hosts (or without IPv6): the TCP listener was bound to `0.0.0.0` (tasks failed to start at all on IPv6-only hosts, and dual-stack hosts never received IPv6 inbound connections); it now prefers the dual-stack `[::]` and adds a plain IPv4 listener on systems with `IPV6_V6ONLY=1` (e.g. the Windows default); the conflict from that extra bind is ignored when the dual-stack socket already covers IPv4
- Fixed UDP trackers being IPv4-only: the socket was bound to `0.0.0.0`, so on IPv6-only hosts every `udp://` tracker was unusable and IPv6 trackers failed to send; it is now dual-stack with the same send-side conversion
- Fixed v4-mapped peer addresses not being normalized: some trackers / PEX implementations send IPv4 addresses as `::ffff:a.b.c.d`, which used to create a second peer record alongside the IPv4 literal and could never be dialed on hosts without an IPv6 route; addresses are now normalized to IPv4 before being stored
- Fixed HTTPS failing across the board with private CAs / corporate proxies (TLS interception): TLS only trusted the Mozilla roots compiled into the binary and the local OS certificate store (corporate CAs, user-imported certificates) took no part in verification; system roots are now loaded as well (a union with the built-in roots, leaving public-site verification results unchanged)
- Fixed the `no-proxy` option having no effect: it was stored but never used to build the HTTP client, so in proxied environments LAN / loopback addresses listed in `no-proxy` were still pushed through the proxy and hosts that were directly reachable failed instead; it now really acts as the proxy's direct-connection exception (when using environment-variable proxies, the `NO_PROXY` environment variable applies)

## Behavior Changes

- DHT KRPC query timeout tightened from 10s to 3s: unreachable nodes fail faster and bootstrapping / get_peers iterations are no longer stalled by slow nodes
- Each HTTP tracker now receives two announce requests per round (IPv4 + IPv6): tracker-side request volume doubles in exchange for all seeders that are only returned to IPv6 sources; no extra request is made when IPv6 is unavailable
- `no-proxy` goes from "recorded only" to actually taking effect: hosts matching that list no longer go through the proxy configured by `all-proxy`
- The HTTPS trust chain is relaxed to the extent of the local system root store (union semantics): certificates trusted by the local machine are accepted by the engine as well, while public-site verification results are unchanged

## Build & Release

- Engine version bumped to 0.3.1 (`engine.getVersion` reports 0.3.1)
- New dependency `rustls-native-certs` (reads the OS certificate store per platform: macOS Security.framework, Windows schannel, Linux /etc/ssl with openssl-probe)