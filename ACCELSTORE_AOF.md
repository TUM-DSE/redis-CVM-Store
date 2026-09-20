AccelStore AOF backend
======================

With `accelstore-config` set, each INCR AOF is an append-only LOG in an embedded
AccelStore store, each BASE/RDB dump a LOG of 4 MiB segments, the manifest a
plain file in `dir`. Durability is the store barrier `accelFsync()`: a metadata
epoch plus its trusted-counter commit.

| `appendfsync` | append | barrier | blocks the event loop |
| --- | --- | --- | --- |
| `always` | inline, after draining the queue | inline | append and barrier |
| `everysec` | bio AOF worker | same worker, once a second | backpressure wait only |
| `no` | bio AOF worker | forced flush, shutdown | backpressure wait only |

Forced flushes (shutdown, BGREWRITEAOF start, `stopAppendOnly`, `BACKUP SEAL`)
and `CONFIG SET appendfsync` drain and reap the queue first, so an inline append
never overtakes a queued one.

Constants:

| Constant | Default | Description |
| --- | --- | --- |
| `ACCEL_AOF_FLUSH_MIN_BYTES` | 256 KiB | Buffer size threshold below which a flush is delayed rather than issued immediately. |
| `ACCEL_AOF_FLUSH_MAX_DELAY_MS` | 100 ms | Longest a sub-threshold buffer is held before being flushed anyway, never past a due everysec barrier. |
| `ACCEL_AOF_WRITE_MAX_PENDING` | 32 | Maximum number of appends queued to the bio AOF worker before backpressure kicks in. |
| `ACCEL_PIPE_BYTES` | 1 MiB | Size of the rewrite pipe used to stream a BASE/RDB dump. |
| `ACCEL_SEGMENT_BYTES` | 4 MiB | Chunk size the rewrite pipe is drained into as LOG segments. |

Reactor naps: after `fork()` every parent write is a copy-on-write fault whose TLB
shootdown IPIs every cpu running a thread of the process, and spinning reactors
always qualify.
`accelstore-nap-us` (immutable, default 50, 0 = never) lets an idle reactor sleep
that long so lazy TLB skips its cpu, only while a fork child lives (`redisFork()`
/ `resetChildState()`).