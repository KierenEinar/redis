//
// Created by kieren jiang on 2023/8/27.
//
#include "server.h"

sds catAppendOnlyFileExpireCommand(sds buf, struct redisCommand *cmd, robj *key, robj *expire) {

    long long when;
    robj *tmpargv[3];
    int j;

    expire = getDecodedObject(expire);
    string2ll(expire->ptr, sdslen(expire->ptr), &when);
    decrRefCount(expire);

    if (cmd->proc == expireCommand) {
        when *= 1000;
    }

    if (cmd->proc == expireCommand || cmd->proc == pexpireCommand) {
        when += mstime();
    }

    tmpargv[0] = createStringObject("PEXPIREAT", 9);
    tmpargv[1] = key;
    tmpargv[2] = createStringObjectFromLongLong(when);
    buf  = catAppendOnlyFileGenericCommand(buf, 3, tmpargv);

    decrRefCount(tmpargv[0]);
    decrRefCount(tmpargv[2]);

    return buf;

}

sds catAppendOnlyFileGenericCommand(sds buf, int argc, robj **argv) {

    int j;

    buf = sdscatfmt(buf, "*%i\r\n", argc);

    for (j = 0; j < argc; j++) {
        robj *obj = argv[j];
        obj = getDecodedObject(obj);
        buf = sdscatfmt(buf, "$%i\r\n", sdslen(obj->ptr));
        buf = sdscatsds(buf, obj->ptr);
        buf = sdscatfmt(buf, shared.crlf->ptr);
        decrRefCount(obj);
    }

    return buf;
}

void feedAppendOnlyFile(struct redisCommand *cmd, int dbid, int argc, robj **argv) {

    sds buf = sdsempty();

    if (dbid != server.aof_seldb) {
        char db_buf[64];
        snprintf(db_buf, sizeof(db_buf), "%d", dbid);
        buf = sdscatfmt(buf, "*2\r\n$6\r\nSELECT\r\n$%U\r\n%i\r\n", strlen(db_buf), dbid);
        server.aof_seldb = dbid;
    }

    if (cmd->proc == expireCommand || cmd->proc == pexpireCommand) {

        buf = catAppendOnlyFileExpireCommand(buf, cmd, argv[1], argv[2]);

    } else if (cmd->proc == setCommand && argc > 3) {

        robj *exargv, *pxargv;

        exargv = NULL;
        pxargv = NULL;

        robj *tempargvs[3];

        for (int j = 3; j < argc; j++) {
            robj *tmpargv = getDecodedObject(argv[j]);
            if (strcasecmp(tmpargv->ptr, "ex") == 0) exargv = argv[j+1];
            if (strcasecmp(tmpargv->ptr, "px") == 0) pxargv = argv[j+1];
            decrRefCount(tmpargv);
        }

        tempargvs[0] = argv[0];
        tempargvs[1] = argv[1];
        tempargvs[2] = argv[2];

        catAppendOnlyFileGenericCommand(buf, 3, tempargvs);

        if (exargv)
            catAppendOnlyFileExpireCommand(buf, server.expireCommand, argv[1], exargv);
        if (pxargv)
            catAppendOnlyFileExpireCommand(buf, server.pexpireCommand, argv[1], pxargv);
    }  else {
        catAppendOnlyFileGenericCommand(buf, argc, argv);
    }

    server.aof_buf = sdscatsds(server.aof_buf, buf);

    sdsfree(buf);

}

ssize_t aofWrite(sds buf, size_t len) {

    ssize_t totwritten = 0, nwritten;

    while (len > 0) {

        nwritten = write(server.aof_fd, buf, len);
        if (nwritten == -1) {
            if (errno == EAGAIN || errno == EINTR) {
                continue;
            }
            return totwritten > 0 ? totwritten : -1;
        }
        totwritten += nwritten;
        buf+=nwritten;
        len-=nwritten;
    }

    return totwritten > 0 ? totwritten : -1;

}

void flushAppendOnlyFile(void) {

    unsigned long sync_in_progress = 0;
    ssize_t nwritten;

    if (sdslen(server.aof_buf) == 0)
        return;

    if (server.aof_fsync == AOF_FSYNC_EVERYSEC) {
        sync_in_progress = bioPendingJobsOfType(BIO_AOF_FSYNC);
        if (sync_in_progress) {
            if (server.aof_postponed_start == 0) {
                server.aof_postponed_start = server.unix_time;
                return;
            }

            if (server.unix_time - server.aof_postponed_start < 2) {
                return;
            }
        }
    }


    nwritten = aofWrite(server.aof_buf, sdslen(server.aof_buf));
    if (nwritten != sdslen(server.aof_buf)) {
        if (nwritten > 0) {
            if (ftruncate(server.aof_fd, server.aof_update_size) == -1) {
                server.aof_update_size += nwritten;
                server.aof_buf = sdsrange(server.aof_buf, nwritten, -1);
            } else {
                nwritten = -1;
            }
        }

        if (server.aof_fsync == AOF_FSYNC_ALWAYS) {
            exit(-1);
        }
        return;
    }

    server.aof_postponed_start = 0;
    server.aof_update_size += nwritten;

    if (sdslen(server.aof_buf) + sdsavail(server.aof_buf) < 4000) {
        server.aof_buf = sdsclear(server.aof_buf);
    } else {
        sdsfree(server.aof_buf);
        server.aof_buf = sdsempty();
    }

    if (server.aof_fsync == AOF_FSYNC_ALWAYS) {
        fsync(server.aof_fd);
        server.aof_last_fsync = server.unix_time;
    } else if (server.aof_fsync == AOF_FSYNC_EVERYSEC && server.aof_last_fsync - server.unix_time > 1){

        if (!sync_in_progress) bioCreateBackgroundJob(BIO_AOF_FSYNC, (void *)(long long)server.aof_fd, NULL, NULL);
        server.aof_last_fsync = server.unix_time;
    }

}


int loadAppendOnlyFile(char *filename) {
    return C_ERR;
}
