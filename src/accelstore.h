/* AccelStore persistence backend: each incr AOF file maps to an append-only
 * LOG, each base file / RDB dump to a LOG of 4 MiB segments streamed through
 * a pipe and a drainer thread, the manifest stays on the filesystem.
 * Enabled when accelstore-config is set (build with ACCELSTORE=yes). */

#ifndef __ACCELSTORE_CLIENT_H
#define __ACCELSTORE_CLIENT_H

#include <stdio.h>
#include <sys/types.h>

/* Streamed segment entry size and the pipe drainer's flush unit. */
#define ACCEL_SEGMENT_BYTES (4 << 20)

/* AOF appends queued on the bio AOF thread before the event loop waits for room. */
#define ACCEL_AOF_WRITE_MAX_PENDING 32

/* Accumulate aof_buf up to this size before appending it, holding it no longer than this. */
#define ACCEL_AOF_FLUSH_MIN_BYTES (256 * 1024)
#define ACCEL_AOF_FLUSH_MAX_DELAY_MS 100

#ifdef USE_ACCELSTORE

int accelEnabled(void);
int accelInit(void);       /* C_OK / C_ERR; call once, after config load */
void accelShutdown(void);  /* stop the store; no ops may be outstanding */

int accelLogCreate(const char *name);              /* -> asfd or -1 */
int accelLogOpenRead(const char *name);            /* eager cache; -> asfd or -1 */
int accelLogOpenAppend(const char *name);          /* lazy cache; -> asfd or -1 */
int accelLogClose(int asfd);
int accelLogDelete(const char *name);
int accelLogExists(const char *name);              /* 1/0 */
ssize_t accelLogAppend(int asfd, const void *buf, size_t len); /* splits > segment */
int accelFsync(void);                              /* global durability barrier */
long long accelLogTotalBytes(const char *name);    /* sum of entry sizes; -1 on error */

/* Whole log, oldest first, as a stdio stream; supports rewind and ftello. */
FILE *accelOpenReadStream(const char *name, long long *total_bytes);

/* Drain pipe_rd to EOF into asfd as segment entries; one drainer at a time. */
int accelStartDrainer(int pipe_rd, int asfd);
int accelJoinDrainer(long long *bytes_out);

const char *accelStrerror(int rc);

#else /* !USE_ACCELSTORE */

#define accelEnabled() 0
static inline int accelInit(void) { return 0; }
static inline void accelShutdown(void) {}
static inline int accelLogCreate(const char *name) { (void)name; return -1; }
static inline int accelLogOpenRead(const char *name) { (void)name; return -1; }
static inline int accelLogOpenAppend(const char *name) { (void)name; return -1; }
static inline int accelLogClose(int asfd) { (void)asfd; return -1; }
static inline int accelLogDelete(const char *name) { (void)name; return -1; }
static inline int accelLogExists(const char *name) { (void)name; return 0; }
static inline ssize_t accelLogAppend(int asfd, const void *buf, size_t len) {
    (void)asfd; (void)buf; (void)len; return -1;
}
static inline int accelFsync(void) { return -1; }
static inline long long accelLogTotalBytes(const char *name) { (void)name; return -1; }
static inline FILE *accelOpenReadStream(const char *name, long long *total_bytes) {
    (void)name; (void)total_bytes; return NULL;
}
static inline int accelStartDrainer(int pipe_rd, int asfd) { (void)pipe_rd; (void)asfd; return -1; }
static inline int accelJoinDrainer(long long *bytes_out) { (void)bytes_out; return -1; }
static inline const char *accelStrerror(int rc) { (void)rc; return "accelstore support not compiled in"; }

#endif /* USE_ACCELSTORE */

#endif /* __ACCELSTORE_CLIENT_H */
