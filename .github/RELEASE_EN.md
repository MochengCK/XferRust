**Summary**

- Fixed HTTP segmented downloads stalling for over ten seconds at 99%: when the last segment's connection went silent, the whole task sat at 99% with zero speed until the regular read-idle timeout (10s) fired, and the tail now uses a shorter timeout

## Bug Fixes

- Fixed HTTP segmented downloads stalling for over ten seconds at 99%: the read-idle timeout was a flat 10s, so once the last segment's connection went silently dead (no data and no FIN), every other worker was already parked and the task sat at 99% with zero speed until the 10s timeout fired and the segment was reconnected from its received watermark — with backoff and reconnect, users saw the progress freeze for over ten seconds (most visible near the end, when no other worker is pulling data). The tail now switches to a 3s read-idle timeout once the remaining bytes drop below the end-game threshold (8MiB), so a dead connection is dropped and reconnected much sooner and the final pause drops from over ten seconds to a few

## Behavior Changes

- The read-idle timeout during the end game (8MiB or less remaining) is tightened from 10s to 3s: slow servers that deliver the tail in bursts may now be disconnected and reconnected a few extra times (a short read does not consume the fatal failure budget, and the segment resumes from the bytes already received, so the task does not fail). Behavior in the middle of a download is unchanged
