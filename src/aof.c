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
        sds s = obj->ptr;
        buf = sdscatfmt(buf, "$%i\r\n", sdslen(s));
        buf = sdscatsds(buf, s);
        buf = sdscatsds(buf, shared.crlf->ptr);
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

        buf = catAppendOnlyFileGenericCommand(buf, 3, tempargvs);

        if (exargv)
            buf = catAppendOnlyFileExpireCommand(buf, server.expire_command, argv[1], exargv);
        if (pxargv)
            buf = catAppendOnlyFileExpireCommand(buf, server.pexpire_command, argv[1], pxargv);
    }  else {
        buf = catAppendOnlyFileGenericCommand(buf, argc, argv);
    }

    if (server.aof_state == AOF_ON)
        server.aof_buf = sdscatsds(server.aof_buf, buf);

    if (server.aof_child_pid != -1)
        aofRewriteBufferAppend(buf);

    sdsfree(buf);

}

void aofRewriteBufferAppend(sds buf) {
    listNode *ln;
    aof_rwblock *rwblock;
    size_t totlen, nlen, len;

    len = sdslen(buf);
    totlen = 0;

    while (len) {
        nlen = 0;
        ln = listLast(server.aof_rw_block_list);
        rwblock = ln ? ln->value : NULL;

        if (rwblock && rwblock->free) {
            nlen = rwblock->free >= len ? len : rwblock->free;
            memcpy(rwblock->buf+rwblock->used, buf+totlen, nlen);
            totlen+=nlen;
            len-=nlen;
        }

        if (len) {
            rwblock = zmalloc(sizeof(*rwblock));
            rwblock->free = AOF_REWRITE_BLOCK_SIZE;
            rwblock->used = 0;
            listAddNodeTail(server.aof_rw_block_list, rwblock);
        }
    }

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

client *createFakeClient(void) {
    client *c;

    c = zmalloc(sizeof(*c));

    c->argc = 0;
    c->argv = NULL;
    selectDb(c, 0);
    initClientMultiState(c);
    c->flag = 0;
    c->reqtype = PROTO_REQ_MULTI;
    c->bulklen = 0l;
    c->multilen = 0l;
    c->querybuf = NULL;
    c->bufpos = 0l;
    c->bufcap = 0l;
    c->reply_bytes = 0l;
    c->cmd = NULL;
    c->client_list_node = NULL;
    c->client_close_node = NULL;
    c->watch_keys = NULL;
    c->reply = listCreate();
    c->fd = -1;
    c->sentlen = 0l;
    return c;

}

void freeFakeClientArgv(int argc, robj **argv) {
    int j;

    for (j=0; j<argc; j++) {
        decrRefCount(argv[j]);
    }

    zfree(argv);
}

void freeFakeClient(client *c) {

    int j;

    freeFakeClientArgv(c->argc, c->argv);

    if (c->querybuf) {
        zfree(c->querybuf);
    }

    zfree(c);
}

void startLoading(FILE *fp) {
    struct stat st;

    fstat(fileno(fp), &st);
    server.loading = 1;
    server.loading_time = time(NULL);
    server.aof_loaded_bytes = 0;
    server.aof_loading_total_bytes = st.st_size;
}

void loadingProgress(off_t pos) {
    server.aof_loaded_bytes = pos;
}

void stopLoading(void) {
    server.loading = 0;
}

int loadAppendOnlyFile(char *filename) {

    client *fake_client;
    struct stat st;
    off_t valid_up_to, valid_up_to_multi;
    int old_state;
    long loops;

    fake_client = createFakeClient();
    FILE *fp = fopen(filename, "r");
    if (fp == NULL || fstat(fileno(fp), &st) == -1) {
        exit(-1);
    }
    if (st.st_size == 0) {
        fclose(fp);
        return C_ERR;
    }

    startLoading(fp);

    old_state = server.aof_state;
    loops = 0;
    server.aof_state = AOF_OFF;

    while (1) {

        char buf[128];
        int argc, j;
        long arglen;
        sds argsds;
        robj **argv;

        struct redisCommand *cmd;

        if ((loops++)%1000 == 0) {
            loadingProgress(ftello(fp));
            processEventsWhileBlocked();
        }

        if (fgets(buf, sizeof(buf), fp) == NULL) {
            if (feof(fp)) {
                break;
            } else {
                goto readerr;
            }
        }

        if (buf[0] != '*') goto fmterr;
        if (buf[1] == '\0') goto readerr;

        argc = atoi(buf+1);
        if (argc < 1) goto fmterr;

        argv = zmalloc(sizeof(robj*) * argc);

        for (j=0; j<argc; j++) {

            if (fgets(buf, sizeof(buf), fp) == NULL) {
                freeFakeClientArgv(j, argv);
                goto readerr;
            }

            if (buf[0] != '$') {
                freeFakeClientArgv(j, argv);
                goto fmterr;
            }
            if (buf[0] == '\0') {
                freeFakeClientArgv(j, argv);
                goto readerr;
            }

            arglen = strtol(buf+1, NULL, 10);
            if (arglen < 1) {
                freeFakeClientArgv(j, argv);
                goto fmterr;
            }

            argsds = sdsnewlen(NULL, arglen);
            if (fread(argsds, arglen, 1, fp) < arglen) {
                sdsfree(argsds);
                freeFakeClientArgv(j, argv);
                goto readerr;
            }

            robj *obj = createObject(REDIS_OBJECT_STRING, argsds);
            argv[j] = obj;
        }

        fake_client->argc = argc;
        fake_client->argv = argv;

        cmd = lookupCommand(fake_client->argv[0]);
        if (cmd == NULL) {
            exit(1);
        }

        if (cmd->proc == multiCommand) valid_up_to_multi = valid_up_to;

        fake_client->cmd = cmd;
        if (fake_client->flag & CLIENT_MULTI && cmd->proc != execCommand) {
            queueMultiCommand(fake_client);
        } else {
            cmd->proc(fake_client);
        }

        if (!(fake_client->flag & CLIENT_MULTI)) {
            valid_up_to = ftello(fp);
        }

        fake_client->cmd = NULL;
        freeFakeClientArgv(fake_client->argc, fake_client->argv);

    }

    if (fake_client->flag & CLIENT_MULTI) {
        valid_up_to = valid_up_to_multi;
        goto uxeof;
    }


loaded_ok:
    fclose(fp);
    aofUpdateCurrentSize();
    freeFakeClient(fake_client);
    server.aof_state = old_state;
    stopLoading();
    return C_OK;
readerr:
    if (!feof(fp)) {
        freeFakeClient(fake_client);
        fclose(fp);
        exit(-1);
    }
uxeof:
    if (server.aof_loaded_truncated) {
        if (valid_up_to > 0 && ftruncate(fileno(fp), valid_up_to) > 0) {
            if (fseek(fp, 0, SEEK_END) == 0) {
                goto loaded_ok;
            }
        }
    }

    freeFakeClient(fake_client);
    fclose(fp);
    exit(-1);
fmterr:
    freeFakeClient(fake_client);
    fclose(fp);
    exit(-1);
}


void aofUpdateCurrentSize(void) {

    struct stat st;

    if (fstat(server.aof_fd, &st) == -1) {
        exit(1);
    }

    server.aof_update_size = st.st_size;
}