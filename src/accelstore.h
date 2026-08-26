/* AccelStore persistence backend (research prototype).
 *
 * Routes Redis's persistence artifacts onto an embedded AccelStore store
 * (SPDK-based authenticated log-structured storage) through the blocking
 * libpyaccelstore.so C ABI:
 *
 *   - each incr AOF file        -> one append-only LOG (one entry per
 *                                  aof_buf flush), fsync -> global barrier
 *   - each base file / RDB dump -> one LOG of 4 MiB segment entries,
 *                                  streamed through a pipe by the writer
 *                                  (forked children never touch the store:
 *                                  they serialize into a pipe, a drainer
 *                                  thread in the parent appends segments)
 *   - manifest / directories    -> stay on the filesystem (tiny, cold)
 *
 * Enabled when accelstore-config is set (build with ACCELSTORE=yes). */

#ifndef __ACCELSTORE_CLIENT_H
#define __ACCELSTORE_CLIENT_H

#include <stdio.h>
#include <sys/types.h>

/* One LOG entry per streamed segment; also the flush unit of the pipe
 * drainer. 4 MiB matches REDIS_AUTOSYNC_BYTES, the rio autosync window. */
#define ACCEL_SEGMENT_BYTES (4 << 20)

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

/* Sequential oldest-to-newest read of a whole log as a stdio stream
 * (fopencookie). Supports rewind to 0 and ftello; *total_bytes (may be NULL)
 * receives the log's total payload size. */
FILE *accelOpenReadStream(const char *name, long long *total_bytes);

/* Pipe drainer: reads pipe_rd until EOF, appending ACCEL_SEGMENT_BYTES
 * entries to asfd. One drainer at a time (Redis runs one saving child at a
 * time). Join returns 0 on success and the byte count. */
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
