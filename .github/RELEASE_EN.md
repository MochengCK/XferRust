**Summary**

- New HLS (M3U8) support: `.m3u8` URLs are downloaded as playlists, with segments fetched concurrently and concatenated into a **single** file in playlist order — no post-processing merge step
- Master playlists pick a stream automatically; fMP4 init segments (`#EXT-X-MAP`), byte-range segments (`#EXT-X-BYTERANGE`) and AES-128 segment encryption are all supported
- Segment sizes are probed up front, so the total length — and therefore progress and ETA — is known
- Resume continues from the last durable contiguous prefix; if the playlist changes, the download restarts from scratch instead of gluing two different playlists together
- Per-task request headers (`header` / `referer` / `user-agent`) now apply to the playlist, the key and every segment as well
- `.m3u8` URLs are now downloaded as playlists by default (see Behavior Changes)

## New Features

### HLS (M3U8) playlist downloads (`.m3u8` URL / `hls` option)

- A `.m3u8` URL is treated as a playlist: the manifest is fetched first, then segments are downloaded concurrently and written **into a single file** in playlist order — the result is the final file, with no separate merge step.
- Master playlists (`#EXT-X-STREAM-INF`) pick a stream automatically, defaulting to the highest bitrate variant.
- Supports `#EXT-X-MAP` (fMP4 init segment; the output is named `.mp4`, otherwise `.ts`), `#EXT-X-BYTERANGE` segments, and `#EXT-X-KEY` AES-128 full-segment encryption (including key rotation; when `IV` is omitted it is derived from that segment's own media sequence number).
- Output naming: when `out` is given explicitly its extension is corrected to match the actual container — `.m3u8` / `.m3u` become `.ts` (`.mp4` when the playlist is fMP4), a missing extension is appended, and any other extension (say an explicit `.mp4`) is left untouched. Without `out` the name is derived from the playlist URL, and a generic last segment such as `index` / `playlist` / `master` falls back to the parent directory name.
- The playlist, the key and every segment share the same set of per-task request headers, so URLs that require `Referer` / `Cookie` work end to end.
- Unsupported cases fail loudly instead of producing a broken file: `SAMPLE-AES` encryption is rejected outright, and when a URL looks like a playlist by extension but the content is not one (a 403 error page, say) the task fails rather than saving that page as a video.
- When a URL does not look like a playlist but the response declares `mpegurl` (e.g. `application/vnd.apple.mpegurl`), it is tried as a playlist first; if the content really is not a playlist, the download falls back to plain HTTP — neither kind of URL fails because of the sniffing.

### HLS options (`hls-variant` / `hls-probe-size` / `hls-segment-retries`)

- `hls-variant=worst` selects the lowest bitrate variant instead of the highest (the default).
- `hls-probe-size=false` disables the segment size probe: saves one round of requests, at the cost of an unknown total length and indeterminate progress.
- `hls-segment-retries` sets the transient-failure retry count for a single segment (3 by default).
- Segment concurrency reuses `split` and `max-connection-per-server` (capped at 32); the per-task `max-download-limit` applies as well.
- `hls=false` forces plain HTTP downloading for a URL whose extension looks like a playlist.

## Behavior Changes

- A URL ending in `.m3u8` is now downloaded as an HLS playlist instead of as a plain file: the result is a concatenated video file (`.ts` by default, `.mp4` for fMP4) rather than the few-hundred-byte manifest text. Pass `hls=false` for that task to keep the old behavior.
- URLs whose response declares `mpegurl` (not necessarily ending in `.m3u8`) are now also tried as a playlist first, and fall back to plain HTTP when the content is not a playlist.
