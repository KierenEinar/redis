//
// Created by kieren jiang on 2023/5/6.
//

#include "server.h"

sds catAppendOnlyGenericCommand(sds dst, robj **argv, int argc) {

    // finally format like this -> pexpireat abc 1683600641

    // *3\r\n
    // $9\r\n
    // PEXPIREAT\r\n
    // $3\r\n
    // abc\r\n
    // $13\r\n
    // 1683600641000\r\n

    char buf[32];
    robj *o;
    buf[0] = '*';
    size_t len = ll2string(buf+1, sizeof(buf) - 1, argc);
    buf[len++] = '\r';
    buf[len++] = '\n';

    dst = sdscatlen(dst, buf, len);
    while (argc>0) {
        buf[0] = '$';
        o = getDecodedObject(argv[argc]);
        len = sdslen(o->ptr);
        size_t slen = ll2string(buf+1, sizeof(buf)-1, len);
        buf[slen++] = '\r';
        buf[slen++] = '\n';
        dst = sdscatlen(dst, buf, slen);
        dst = sdscatlen(dst, o->ptr, sdslen(o->ptr));
        dst = sdscatlen(dst, "\r\n", 2);
        decrRefCount(o);
        argc--;
    }

    return dst;
}

sds catAppendOnlyExpireAtCommand(sds buf, struct redisCommand *cmd, robj *key, robj *seconds) {
    long long when;
    robj *argv[3];
    seconds = getDecodedObject(seconds);
    if (!string2ll(seconds->ptr, sdslen(seconds->ptr), &when)) {
        // todo server panic
    }

    if (cmd->proc == expireCommand || cmd->proc == expireatCommand || cmd->proc == setexCommand) {
        when = when * 1000;
    }

    if (cmd->proc == expireCommand || cmd->proc == pexpireCommand ||
        cmd->proc == setexCommand || cmd->proc == psetexCommand ) {
        when = ms_now() + when;
    }

    argv[0] = createStringObject("PEXPIREAT", 9);
    argv[1] = key;
    argv[2] = createStringObjectFromLongLong(when);

    decrRefCount(seconds);

    buf = catAppendOnlyGenericCommand(buf, argv, 3);

    decrRefCount(argv[0]);
    decrRefCount(argv[2]);

    return buf;
}

size_t aofwrite(int fd, sds buf, size_t len) {
    size_t towrite = 0, n = 0;
    while (len) {
        n = write(fd, buf, len);
        if (n < 0) {
            if (errno == EINTR) {
                continue;
            }
            return towrite ? towrite : -1;
        }
        towrite += n;
        len-=n;
        buf+=n;
    }
    return towrite;
}


void feedAppendOnlyFile(struct redisCommand *cmd, int seldb, robj **argv, int argc) {

    sds buf = sdsempty();
    robj *tmpargv[3];

    // select db
    if (server.select_db != seldb) {
        char s[64];
        size_t slen = ll2string(s, 64, seldb);
        buf = sdscatfmt(buf, "*2\r\n$6\r\nSELECT\r\n$%d\r\n%s\r\n", seldb,
                        (int)slen, s);
        server.select_db = seldb;
    }

    if (cmd->proc == expireCommand || cmd->proc == expireAtCommand ||
        cmd->proc == pexpireCommand || cmd->proc == pexpireAtCommand ) {

        buf = catAppendOnlyExpireAtCommand(buf, cmd, argv[1], argv[2]);
    } else if (cmd->proc == setexCommand) {

        tmpargv[0] = createStringObject("SET", 3);
        tmpargv[1] = argv[1];
        tmpargv[2] = argv[2];
        buf = catAppendOnlyGenericCommand(buf, tmpargv, 3);
        decrRefCount(tmpargv[0]);
        buf = catAppendOnlyExpireAtCommand(buf, cmd, argv[1], argv[2]);

    } else if (cmd->proc == setCommand && argc > 3) {

        robj *exarg = NULL;
        robj *pxarg = NULL;

        for (int i=3; i<argc; i++) {
            if (!strcasecmp("ex", argv[i]->ptr)) exarg = argv[i+1];
            if (!strcasecmp("px", argv[i]->ptr)) pxarg = argv[i+1];
        }

        // todo assert (exarg || pxarg) && !(exarg && pxarg)
        tmpargv[0] = createStringObject("SET", 3);
        tmpargv[1] = argv[1];
        tmpargv[2] = argv[2];
        buf = catAppendOnlyGenericCommand(buf, tmpargv, 3);
        decrRefCount(tmpargv[0]);

        if (exarg)
            buf = catAppendOnlyExpireAtCommand(buf, expireCommand, argv[1], argv[4]);

        if (pxarg)
            buf = catAppendOnlyExpireAtCommand(buf, pexpireCommand, argv[1], argv[4]);

    } else {
        buf = catAppendOnlyGenericCommand(buf, argv, argc);
    }

    if (server.aof_state == AOF_ON) {
        server.aof_buf = sdscatsds(server.aof_buf, buf);
    }

    if (server.aof_child_pid != -1) {
        aofRewriteBufferAppend((char *)buf, sdslen(buf));
    }

    sdsfree(buf);

}

void flushAppendOnlyFile(int force) {

    if (server.aof_buf == 0) {
        return;
    }

    int sync_in_progress = 0;

    if (server.aof_fsync == AOF_FSYNC_EVERYSEC) {
        sync_in_progress = bioPendingJobsOfType(BIO_AOF_FSYNC);
    }

    if (server.aof_buf && !force) {
        if (sync_in_progress) {
            if (server.aof_flush_postponed_start == 0) {
                server.aof_flush_postponed_start = server.unix_time;
                return;
            } else if (server.unix_time - server.aof_flush_postponed_start < 2) {
                return;
            }

            server.aof_fsync_delayed++;
        }
    }

    server.aof_flush_postponed_start = 0;

    size_t nwritten = aofwrite(server.aof_fd, server.aof_buf, (size_t)sdslen(server.aof_buf));

    if (nwritten != (size_t) sdslen(server.aof_buf)) {

        if (nwritten == -1) {
            server.aof_last_write_errno = errno;
        } else {

            if (ftruncate(server.aof_fd, server.aof_current_size) == -1) { // truncate failed
                server.aof_current_size += nwritten;
                server.aof_buf = sdsrange(server.aof_buf, nwritten, -1);
            }

            server.aof_last_write_errno = ENOSPC;
        }

        server.aof_last_write_status = REDIS_ERR;

        if (server.aof_fsync == AOF_FSYNC_ALWAYS) {
            exit(1);
        }

        return;

    }

    if (server.aof_last_write_status == REDIS_ERR)
        server.aof_last_write_status = REDIS_OK;

    server.aof_current_size += nwritten;

    if (sdsavail(server.aof_buf) + sdslen(server.aof_buf) < 4000) {
        sdsclear(server.aof_buf);
    } else {
        sdsfree(server.aof_buf);
        server.aof_buf = sdsempty();
    }

    if (server.aof_fsync == AOF_FSYNC_ALWAYS) {
        fsync(server.aof_fd);
    } else if (server.aof_fsync == AOF_FSYNC_EVERYSEC && server.unix_time > server.aof_last_fsync ) {
        if (!sync_in_progress)
            bioCreateBackgroundJob(BIO_AOF_FSYNC, server.aof_fd, NULL, NULL);
        server.aof_last_fsync = server.unix_time;
    }
}

