/* AccelStore persistence backend implementation. See accelstore.h. */

#include "server.h"
#include "accelstore.h"

#ifdef USE_ACCELSTORE

#include <pthread.h>
#include <unistd.h>
#include <errno.h>
#include <string.h>
#include <stdlib.h>
#include <fcntl.h>
#include <sys/resource.h>
#include <sys/syscall.h>

#include "bio.h"
#include "pyas_shim.h"

static struct pyas_store *accel_store = NULL;

/* Set when the rewrite child and the drainer share the event loop's cpus. */
static int accel_rewrite_shared = 0;
#define ACCEL_REWRITE_NICE 10

/* Render a cpu set as a taskset-style list ("0-3,7"). */
static void accelFormatCpuList(const cpu_set_t *set, char *out, size_t outlen) {
    size_t used = 0;
    int cpu = 0;

    out[0] = '\0';
    while (cpu < CPU_SETSIZE) {
        int first, last;

        if (!CPU_ISSET(cpu, set)) { cpu++; continue; }
        first = cpu;
        while (cpu + 1 < CPU_SETSIZE && CPU_ISSET(cpu + 1, set)) cpu++;
        last = cpu++;

        int n = first == last ?
            snprintf(out + used, outlen - used, "%s%d", used ? "," : "", first) :
            snprintf(out + used, outlen - used, "%s%d-%d", used ? "," : "", first, last);
        if (n < 0 || (size_t)n >= outlen - used) break;
        used += (size_t)n;
    }
}

/* Lower the calling thread's priority (per thread on Linux, no capability). */
static void accelNiceSelf(const char *what) {
    errno = 0;
    if (setpriority(PRIO_PROCESS, (id_t)syscall(SYS_gettid), ACCEL_REWRITE_NICE) == -1)
        serverLog(LL_VERBOSE, "accelstore: can't nice %s: %s", what, strerror(errno));
    else
        serverLog(LL_VERBOSE, "accelstore: %s runs at nice +%d", what,
                  ACCEL_REWRITE_NICE);
}

/* Move the main thread and the bio workers off the reactor cpus and decide
 * where the AOF rewrite child and its drainer go. Called once from accelInit(). */
static void accelPlaceThreadsOffReactorCores(void) {
    unsigned long long mask = strtoull(server.accel_core_mask, NULL, 0);
    char cpulist[512], rewritelist[512];
    cpu_set_t cur, trimmed, rewrite;
    const char *source, *rewrite_unit = "cpus";
    int bio_failed;

    if (mask == 0) return;
    if (pthread_getaffinity_np(pthread_self(), sizeof(cur), &cur) != 0) return;

    trimmed = cur;
    for (int cpu = 0; cpu < 64; cpu++)
        if (mask & (1ULL << cpu)) CPU_CLR(cpu, &trimmed);
    if (CPU_COUNT(&trimmed) == 0) {
        serverLog(LL_WARNING,
                  "accelstore: reactor core mask %s covers every cpu this "
                  "process may run on, leaving the event loop on the reactor "
                  "cores", server.accel_core_mask);
        return;
    }

    /* An explicit aof-rewrite-cpulist is taken as given. */
    if (server.aof_rewrite_cpulist && server.aof_rewrite_cpulist[0] != '\0') {
        redis_strlcpy(rewritelist, server.aof_rewrite_cpulist, sizeof(rewritelist));
        source = "aof-rewrite-cpulist";
    } else if (CPU_COUNT(&trimmed) >= 3) {
        int top = -1;

        for (int cpu = 0; cpu < CPU_SETSIZE; cpu++)
            if (CPU_ISSET(cpu, &trimmed)) top = cpu;
        CPU_ZERO(&rewrite);
        CPU_SET(top, &rewrite);
        CPU_CLR(top, &trimmed);
        accelFormatCpuList(&rewrite, rewritelist, sizeof(rewritelist));
        source = "derived";
        rewrite_unit = "cpu";
    } else {
        /* Two cpus or fewer: share with the event loop, niced. */
        accelFormatCpuList(&trimmed, rewritelist, sizeof(rewritelist));
        accel_rewrite_shared = 1;
        source = "shared, deprioritised";
    }
    if (server.aof_rewrite_cpulist == NULL)
        server.aof_rewrite_cpulist = zstrdup(rewritelist);
    if (server.bgsave_cpulist == NULL)
        server.bgsave_cpulist = zstrdup(rewritelist);

    accelFormatCpuList(&trimmed, cpulist, sizeof(cpulist));

    int rc = pthread_setaffinity_np(pthread_self(), sizeof(trimmed), &trimmed);
    if (rc != 0) {
        serverLog(LL_WARNING, "accelstore: can't move the main thread off the "
                              "reactor cores: %s", strerror(rc));
        return;
    }

    /* The bio workers already exist (InitServerLast), so re-apply their
     * affinity; an explicit bio-cpulist is left alone. */
    if (server.bio_cpulist == NULL) {
        bio_failed = bioSetWorkerAffinity(&trimmed, sizeof(trimmed));
        if (bio_failed)
            serverLog(LL_WARNING, "accelstore: %d bio worker(s) could not be "
                                  "moved off the reactor cores", bio_failed);
        server.bio_cpulist = zstrdup(cpulist);
    }

    serverLog(LL_NOTICE,
              "accelstore: reactor cores %s reserved; main thread and bio "
              "workers on cpus %s%s; AOF rewrite child and drainer on %s %s (%s)",
              server.accel_core_mask, cpulist,
              server.bio_cpulist && strcmp(server.bio_cpulist, cpulist) ?
                  " (bio workers on the configured bio-cpulist)" : "",
              rewrite_unit, rewritelist, source);
}

/* Called by a saving child after redisSetCpuAffinity(). */
void accelDeprioritiseChild(void) {
    if (!accel_rewrite_shared) return;
    accelNiceSelf("the saving child");
}

/* Widen a rewrite pipe; best effort (F_SETPIPE_SZ is capped by pipe-max-size). */
void accelSetPipeSize(int fd) {
    int got = fcntl(fd, F_SETPIPE_SZ, ACCEL_PIPE_BYTES);

    if (got == -1) {
        int err = errno;
        got = fcntl(fd, F_GETPIPE_SZ);
        serverLog(LL_VERBOSE, "accelstore: rewrite pipe stays at %d bytes "
                              "(F_SETPIPE_SZ %d: %s)", got, ACCEL_PIPE_BYTES,
                              strerror(err));
    } else {
        serverLog(LL_VERBOSE, "accelstore: rewrite pipe buffer is %d bytes", got);
    }
}

int accelEnabled(void) {
    return server.accel_config != NULL && server.accel_config[0] != '\0';
}

const char *accelStrerror(int rc) {
    return pyas_error_string(rc);
}

int accelInit(void) {
    serverAssert(accel_store == NULL);
    if (!accelEnabled()) return C_OK;
    if (server.accel_device == NULL || server.accel_device[0] == '\0') {
        serverLog(LL_WARNING, "accelstore-config set but accelstore-device missing");
        return C_ERR;
    }
    if (server.daemonize) {
        serverLog(LL_WARNING, "accelstore persistence cannot run with daemonize yes "
                              "(the store does not survive the daemonize fork)");
        return C_ERR;
    }

    struct pyas_kv sync_opts[] = {
        {.key = "trigger", .value = "on-barrier"},
    };
    struct pyas_opts opts;
    memset(&opts, 0, sizeof(opts));
    opts.abi_version = PYAS_ABI_VERSION;
    opts.module = PYAS_MODULE_LOG;
    opts.workers = (uint32_t)server.accel_workers;
    int have_mask = server.accel_core_mask && server.accel_core_mask[0] != '\0';
    if (have_mask)
        opts.core_mask = server.accel_core_mask;
    else
        opts.cores = (uint32_t)server.accel_cores;
    opts.tc_backend = server.accel_tc_rtt_us > 0 ? PYAS_TC_BACKEND_MOCK_REMOTE
                                                 : PYAS_TC_BACKEND_LOCAL;
    opts.tc_rtt_model = PYAS_TC_RTT_EVERY;
    opts.tc_rtt_us = (uint64_t)(server.accel_tc_rtt_us > 0 ? server.accel_tc_rtt_us : 0);
    opts.silent = 1;
    opts.json_config_path = server.accel_config;
    opts.bdev_name = server.accel_device;
    /* Trusted counter beside the data files; the store refuses to load without it. */
    opts.tc_persist_path = "accelstore.tc";
    opts.sync_opts = sync_opts;
    opts.sync_opts_count = sizeof(sync_opts) / sizeof(sync_opts[0]);
    /* One streamed segment plus headroom; accelLogAppend() splits larger appends. */
    opts.max_op_bytes = 2ULL * ACCEL_SEGMENT_BYTES;
    opts.cluster_size = server.accel_cluster_size;
    opts.md_pages = (uint64_t)server.accel_md_pages;

    int rc = pyas_start(&opts, &accel_store);
    if (rc != 0) {
        serverLog(LL_WARNING, "accelstore: store start failed: %s", pyas_error_string(rc));
        return C_ERR;
    }
    serverLog(LL_NOTICE,
              "accelstore: store up (device %s, workers %u, tc %s%s, "
              "fork-child reactor nap %d us)",
              server.accel_device, pyas_workers(accel_store),
              server.accel_tc_rtt_us > 0 ? "mock-remote" : "local",
              server.accel_tc_rtt_us > 0 ? " rtt" : "",
              server.accel_nap_us);
    serverLog(LL_NOTICE,
              "accelstore: AOF appends run on the bio AOF thread unless "
              "appendfsync is always or the flush is forced "
              "(ACCEL_AOF_FLUSH_MIN_BYTES=%d KiB, "
              "ACCEL_AOF_FLUSH_MAX_DELAY_MS=%d, "
              "ACCEL_AOF_WRITE_MAX_PENDING=%d)",
              ACCEL_AOF_FLUSH_MIN_BYTES / 1024,
              ACCEL_AOF_FLUSH_MAX_DELAY_MS,
              ACCEL_AOF_WRITE_MAX_PENDING);

    /* SPDK reactors spin at 100%; keep our threads off their cores. */
    if (have_mask) accelPlaceThreadsOffReactorCores();
    return C_OK;
}

void accelShutdown(void) {
    if (accel_store == NULL) return;
    int rc = pyas_stop(accel_store);
    if (rc != 0)
        serverLog(LL_WARNING, "accelstore: store stop failed: %s", pyas_error_string(rc));
    accel_store = NULL;
}

/* ------------------------------------------------------------------ LOG ops */

int accelLogCreate(const char *name) {
    int fd = -1;
    int rc = pyas_log_create(accel_store, name, &fd);
    if (rc != 0) {
        serverLog(LL_WARNING, "accelstore: create %s failed: %s", name, pyas_error_string(rc));
        errno = -rc > 0 ? -rc : EIO;
        return -1;
    }
    return fd;
}

static int accelLogOpenMode(const char *name, int cache) {
    int fd = -1;
    int rc = pyas_log_open(accel_store, name, cache, &fd);
    if (rc != 0) {
        errno = -rc > 0 ? -rc : EIO;
        return -1;
    }
    return fd;
}

int accelLogOpenRead(const char *name) {
    return accelLogOpenMode(name, PYAS_LOG_CACHE_EAGER);
}

int accelLogOpenAppend(const char *name) {
    return accelLogOpenMode(name, PYAS_LOG_CACHE_LAZY);
}

int accelLogClose(int asfd) {
    int rc = pyas_log_close(accel_store, asfd);
    if (rc != 0) {
        serverLog(LL_WARNING, "accelstore: close fd %d failed: %s", asfd, pyas_error_string(rc));
        errno = -rc > 0 ? -rc : EIO;
        return -1;
    }
    return 0;
}

int accelLogDelete(const char *name) {
    int rc = pyas_log_delete(accel_store, name);
    if (rc != 0) {
        serverLog(LL_WARNING, "accelstore: delete %s failed: %s", name, pyas_error_string(rc));
        errno = -rc > 0 ? -rc : EIO;
        return -1;
    }
    return 0;
}

int accelLogExists(const char *name) {
    /* A lazy open builds no cache: cheapest existence probe. */
    int fd = accelLogOpenMode(name, PYAS_LOG_CACHE_LAZY);
    if (fd < 0) return 0;
    accelLogClose(fd);
    return 1;
}

ssize_t accelLogAppend(int asfd, const void *buf, size_t len) {
    /* One LOG entry per flush; larger flushes are split into segment-sized entries. */
    size_t done = 0;
    while (done < len) {
        size_t n = len - done;
        if (n > ACCEL_SEGMENT_BYTES) n = ACCEL_SEGMENT_BYTES;
        int rc = pyas_log_append(accel_store, asfd, (const char *)buf + done, n);
        if (rc != 0) {
            serverLog(LL_WARNING, "accelstore: append %zu bytes failed: %s", n,
                      pyas_error_string(rc));
            errno = -rc > 0 ? -rc : EIO;
            /* Entries are all-or-nothing: report what landed, like write(2). */
            return done > 0 ? (ssize_t)done : -1;
        }
        done += n;
    }
    return (ssize_t)len;
}

int accelFsync(void) {
    int rc = pyas_fsync(accel_store);
    if (rc != 0) {
        serverLog(LL_WARNING, "accelstore: fsync barrier failed: %s", pyas_error_string(rc));
        errno = -rc > 0 ? -rc : EIO;
        return -1;
    }
    return 0;
}

/* Idle reactors sleep in the kernel while a fork child lives, so the
 * parent's copy-on-write faults do not IPI their cpus. */
void accelSetNap(int on) {
    static int nap_on = 0;

    if (accel_store == NULL || server.accel_nap_us <= 0) return;
    if (on == nap_on) return;

    int rc = pyas_set_nap(accel_store, on ? (uint32_t)server.accel_nap_us : 0u);
    if (rc != 0) {
        /* State unchanged: a later call for the same value retries. */
        serverLog(LL_WARNING, "accelstore: could not %s reactor naps: %s",
                  on ? "enable" : "disable", pyas_error_string(rc));
        return;
    }
    nap_on = on;
    if (on)
        serverLog(LL_NOTICE, "accelstore: reactor naps on (%d us) while the fork "
                             "child runs", server.accel_nap_us);
    else
        serverLog(LL_NOTICE, "accelstore: reactor naps off");
}

long long accelLogTotalBytes(const char *name) {
    int fd = accelLogOpenMode(name, PYAS_LOG_CACHE_LAZY);
    if (fd < 0) return -1;
    uint64_t count = 0;
    long long total = 0;
    int rc = pyas_log_length(accel_store, fd, &count);
    if (rc != 0) {
        accelLogClose(fd);
        return -1;
    }
    for (uint64_t pos = 0; pos < count; pos++) {
        uint64_t sz = 0;
        rc = pyas_log_retrieve_size(accel_store, fd, pos, &sz);
        if (rc != 0) {
            accelLogClose(fd);
            return -1;
        }
        total += (long long)sz;
    }
    accelLogClose(fd);
    return total;
}

/* ------------------------------------------------- sequential read streams */

typedef struct accelReadStream {
    int asfd;
    uint64_t count;   /* entries in the log */
    uint64_t next;    /* next entry INDEX to fetch, 0 = oldest */
    char *buf;        /* current entry payload */
    size_t cap, len, off;
    long long logical; /* stream position for ftello */
} accelReadStream;

static ssize_t accelStreamRead(void *cookie, char *out, size_t want) {
    accelReadStream *s = cookie;
    size_t done = 0;
    while (done < want) {
        if (s->off == s->len) {
            if (s->next >= s->count) break; /* EOF */
            /* pos 0 is the NEWEST entry; oldest-first index i is count-1-i. */
            uint64_t pos = s->count - 1 - s->next;
            uint64_t sz = 0;
            int rc = pyas_log_retrieve_size(accel_store, s->asfd, pos, &sz);
            if (rc != 0) {
                errno = -rc > 0 ? -rc : EIO;
                return done > 0 ? (ssize_t)done : -1;
            }
            if (sz > s->cap) {
                char *nb = zrealloc(s->buf, sz);
                s->buf = nb;
                s->cap = sz;
            }
            uint64_t got = 0;
            rc = pyas_log_retrieve(accel_store, s->asfd, pos, s->buf, s->cap, &got);
            if (rc != 0) {
                errno = -rc > 0 ? -rc : EIO;
                return done > 0 ? (ssize_t)done : -1;
            }
            s->len = got;
            s->off = 0;
            s->next++;
            if (got == 0) continue;
        }
        size_t n = s->len - s->off;
        if (n > want - done) n = want - done;
        memcpy(out + done, s->buf + s->off, n);
        s->off += n;
        done += n;
    }
    s->logical += (long long)done;
    return (ssize_t)done;
}

static int accelStreamSeek(void *cookie, off64_t *offset, int whence) {
    accelReadStream *s = cookie;
    if (whence == SEEK_CUR && *offset == 0) {
        *offset = s->logical;
        return 0;
    }
    if (whence == SEEK_SET && *offset == 0) {
        /* Rewind after the AOF loader's magic sniff. */
        s->next = 0;
        s->len = s->off = 0;
        s->logical = 0;
        return 0;
    }
    errno = ESPIPE;
    return -1;
}

static int accelStreamClose(void *cookie) {
    accelReadStream *s = cookie;
    if (s->asfd >= 0) accelLogClose(s->asfd);
    zfree(s->buf);
    zfree(s);
    return 0;
}

FILE *accelOpenReadStream(const char *name, long long *total_bytes) {
    int fd = accelLogOpenRead(name);
    if (fd < 0) return NULL;

    accelReadStream *s = zcalloc(sizeof(*s));
    s->asfd = fd;
    uint64_t count = 0;
    int rc = pyas_log_length(accel_store, fd, &count);
    if (rc != 0) {
        accelLogClose(fd);
        zfree(s);
        errno = -rc > 0 ? -rc : EIO;
        return NULL;
    }
    s->count = count;
    if (total_bytes) {
        long long total = 0;
        for (uint64_t pos = 0; pos < count; pos++) {
            uint64_t sz = 0;
            rc = pyas_log_retrieve_size(accel_store, fd, pos, &sz);
            if (rc != 0) {
                accelLogClose(fd);
                zfree(s);
                errno = -rc > 0 ? -rc : EIO;
                return NULL;
            }
            total += (long long)sz;
        }
        *total_bytes = total;
    }

    cookie_io_functions_t io = {
        .read = accelStreamRead,
        .write = NULL,
        .seek = accelStreamSeek,
        .close = accelStreamClose,
    };
    FILE *fp = fopencookie(s, "r", io);
    if (fp == NULL) {
        accelStreamClose(s);
        return NULL;
    }
    return fp;
}

/* ------------------------------------------------------------ pipe drainer */

typedef struct accelDrainer {
    pthread_t tid;
    int pipe_rd;
    int asfd;
    long long bytes;
    int err;      /* 0 or errno */
    int active;
} accelDrainer;

static accelDrainer drainer = {.active = 0};

static void *accelDrainerMain(void *arg) {
    accelDrainer *d = arg;
    char *seg;

    /* The drainer is the rewrite child's peer: same cpus, niced when shared. */
    redisSetCpuAffinity(server.aof_rewrite_cpulist);
    if (accel_rewrite_shared) accelNiceSelf("the AOF rewrite drainer");

    seg = zmalloc(ACCEL_SEGMENT_BYTES);
    size_t fill = 0;
    for (;;) {
        ssize_t n = read(d->pipe_rd, seg + fill, ACCEL_SEGMENT_BYTES - fill);
        if (n < 0) {
            if (errno == EINTR) continue;
            d->err = errno;
            break;
        }
        if (n == 0) { /* writer closed: flush the tail */
            if (fill > 0 && accelLogAppend(d->asfd, seg, fill) != (ssize_t)fill)
                d->err = errno ? errno : EIO;
            else
                d->bytes += (long long)fill;
            break;
        }
        fill += (size_t)n;
        if (fill == ACCEL_SEGMENT_BYTES) {
            if (accelLogAppend(d->asfd, seg, fill) != (ssize_t)fill) {
                d->err = errno ? errno : EIO;
                break;
            }
            d->bytes += (long long)fill;
            fill = 0;
        }
    }
    close(d->pipe_rd);
    zfree(seg);
    return NULL;
}

int accelStartDrainer(int pipe_rd, int asfd) {
    serverAssert(!drainer.active);
    drainer.pipe_rd = pipe_rd;
    drainer.asfd = asfd;
    drainer.bytes = 0;
    drainer.err = 0;
    drainer.active = 1;
    if (pthread_create(&drainer.tid, NULL, accelDrainerMain, &drainer) != 0) {
        drainer.active = 0;
        return -1;
    }
    return 0;
}

int accelJoinDrainer(long long *bytes_out) {
    serverAssert(drainer.active);
    pthread_join(drainer.tid, NULL);
    drainer.active = 0;
    if (bytes_out) *bytes_out = drainer.bytes;
    if (drainer.err) {
        errno = drainer.err;
        return -1;
    }
    return 0;
}

#endif /* USE_ACCELSTORE */
