**Summary**

- HLS streams (a playlist with no `#EXT-X-ENDLIST`) are now **recorded continuously**: instead of downloading the current window and stopping, the engine follows new segments as the manifest grows and appends them in order until the stream ends or the source stops publishing
- Recording is **resumable**: pausing (or failing and retrying) keeps everything recorded so far, and resuming continues from the break point — signed URLs that rotate on every fetch do not get in the way
- Task status reports live recording: `isLive` marks a recording task, and `liveRecordedMs` is the **recorded media duration** (how much of the output is playable), so clients can show "recording · elapsed"
- New global / per-task option `hls-live-stall-timeout` (seconds): how long a live source may go without new segments before the recording wraps up; 120 seconds by default, `0` = never conclude on inactivity
- Nothing is silently faked: if the live window slides past a segment before it could be fetched, the engine skips it openly (and logs it, without failing the task); if a segment can never be fetched and blocks the splice point, the task fails honestly and a retry continues from the break point

## New Features

### HLS live recording

- A playlist without `#EXT-X-ENDLIST` is treated as an ongoing recording: the manifest is reloaded at half the `#EXT-X-TARGETDURATION` (clamped to 0.5–30 seconds) and new segments are recognised by their **media sequence** — hotlink-protected streams whose URLs rotate on every fetch are unaffected.
- Segments are downloaded concurrently and spliced strictly in order: out-of-order segments land in per-segment files first and are moved into place at the splice point, so the output always stays a playable prefix; `#EXT-X-MAP` init segments (including ones replaced mid-recording) are handled too.
- The recording wraps up when: the manifest gains `#EXT-X-ENDLIST`; no new segment shows up for `hls-live-stall-timeout` seconds (the source stopped publishing, or the playlist was never really live); the manifest keeps returning 404 / 410; or the task is paused / removed.
- Resuming shares the same on-disk state as regular playlist downloads: the control file gains a live record (bytes on disk, next media sequence to splice, recorded duration, init segment already written). Pausing and failing both keep it; resuming truncates the output to the last flushed watermark, clears stale segment files, and re-fetches the remaining window with `Range` requests.

## Behavior Changes

- The default behaviour for live / rolling-window playlists changes from "download the current window snapshot" to **recording until wrapped up**: callers that used such URLs as "grab this window and go" will see the task keep running — which is the point of this release; pause the task to stop it (the state is kept).
- The output is "what was actually recorded": a window missed during recording is reported as lost (the log says how many segments were skipped), never padded with placeholders, and never fails the task by itself.
- A plain VOD playlist without `#EXT-X-ENDLIST` also goes through the recording path: it finishes after `hls-live-stall-timeout` (120 seconds by default) with nothing new, instead of finishing instantly.

(Low-latency playlists consisting only of `#EXT-X-PART` are outside the scope of continuous recording: their parts do not have stable identities, so they keep the current-window snapshot behaviour.)
