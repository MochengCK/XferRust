**Summary**

- HLS (M3U8) tasks no longer "run for a while and then fail": one failing segment no longer condemns the whole task — the remaining segments are still downloaded, and the progress made after the failing one is not thrown away
- Re-downloading no longer starts from zero: when the manifest's segment URLs carry a signature or an expiry (`?sign=…&t=…`), every fresh manifest fetch returns a new set of URLs, and the resume state is no longer discarded because of it
- A failed task can be **resumed in place**: the output file, the control file and the segment directory are all kept, so the task continues from the break point instead of being deleted and restarted
- Expired segment URLs (401 / 403 / 410) no longer fail the task outright: the engine fetches the manifest again, picks up fresh URLs and carries on
- Transient-failure retries for playlists are wider and longer: 401 / 403 / 408 / 410 / 425 / 429 all count as transient, the retry budget goes from 3 to 6 attempts, and the backoff becomes 1 / 2 / 4 / 8 / 15 seconds
- HLS URL detection is more tolerant: playlist endpoints without a `.m3u8` extension (`?format=hls` / `?type=m3u8` style relay URLs) are now downloaded as playlists too
- New BT playhead priority: while streaming a torrent, the pieces around the playback position are fetched first, so playback starts sooner
- Fixed the session file being torn apart by concurrent saves: several background saves sharing one temp file could leave a half-written JSON behind, which on the next start means the subscriptions, the tracker list and every task are **lost**

## New Features

### BT playhead priority (`task.setPlayhead`)

- The caller reports "the playhead is currently at this byte offset" and the engine fetches the pieces around that position first, filling in the rest in the usual order.
- This is a best-effort hint: a paused task, a magnet whose metadata is not ready yet and a non-BT task do not raise an error, the hint simply does not apply (the call returns `applied: false`); a task that does not exist still reports an error as before.

### More tolerant HLS URL detection (playlist endpoints without an extension)

- Any one of three hints is enough to try the URL as a playlist: a `.m3u8` / `.m3u` path suffix, a response that declares an mpegurl content type, or a query string that says HLS outright (`?format=hls` / `?type=m3u8` / `?output=m3u8` and friends). Plenty of relay endpoints serve a playlist with no extension at all, and the last hint is what covers them.
- These are only hints, so the body is still checked for a leading `#EXTM3U`: when the content really is not a playlist, a URL without a `.m3u8` extension falls back to a plain file download, while a URL that does end in `.m3u8` fails honestly — such a URL returning non-playlist content is almost always a login page or an interception page, and saving it as a "video" is worse than failing.

## Bug Fixes

- Fixed an HLS task failing entirely because of one bad segment: a single segment hitting a run of 5xx responses used to end the task immediately and invalidate every segment already downloaded before it. The failing segment is now recorded, the remaining segments are still downloaded, and the real error is reported once the rest is done — so a failure keeps as much progress as possible and a retry only has to fetch what is still missing.
- Fixed re-downloading always starting from zero: the control file now records two manifest fingerprints — one comparing URLs literally, one ignoring the credential-style parameters that change on every fetch (`sign` / `token` / `expires` and so on). On a site with hotlink protection, a re-fetched manifest that differs only by its signature no longer invalidates the resume state.
- Fixed expired segment URLs failing the task: `401` / `403` / `408` / `410` / `425` / `429` now count as transient, and a retry fetches the manifest again — picking up a fresh set of URLs — before continuing.
- Fixed a failed task being unable to continue: `task.resume` now also accepts the failed state, so the task restarts in place with its output path, control file and segment directory intact, resuming from the break point.
- Fixed unhelpful error reporting around resumes: a failing segment now reports its own error (say `HTTP 403`) instead of a blanket "download incomplete". That is what lets a caller decide whether retrying is worthwhile.
- Fixed the session file being torn apart by concurrent saves: the periodic save, the subscription refresh loop and task teardown all save the session, and saving is "write a temp file, then rename". When two saves share the same temp file name they truncate each other and race for the rename, leaving a half-written JSON behind — the next start fails to parse it, ignores the whole session, and **every subscription, tracker and task is lost**. Saves are serialized now, so each one lands a complete file.

## Behavior Changes

- When one segment of an HLS task fails, the remaining segments are still downloaded (previously the task stopped immediately). This only maximises the data already obtained: the task status is still failed, a retry continues from the break point, and an output with missing segments is never produced.
- The transient-failure retry budget for playlists goes from 3 to 6 attempts, and the backoff changes from a fixed 1 / 3 seconds to 1 / 2 / 4 / 8 / 15 seconds. The rule for plain HTTP tasks is unchanged: a single file has no "fetch a fresh set of URLs" step, so retrying a 4xx is pointless.
- `task.resume` now works on failed tasks as well — a failure no longer means "delete it and start over".
