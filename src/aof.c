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
    if (len == 0)
        return;

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

    if (elGetFileEvent(server.el, server.aof_pipe_write_data_to_child, EL_WRITABLE) == 0)
        elCreateFileEvent(server.el,  server.aof_pipe_write_data_to_child,
                          EL_WRITABLE, aofRewriteBufferPipeWritable, NULL);

}

size_t aofRewriteBufferWrite(int fd) {

    listNode *node = listFirst(server.aof_rw_block_list);
    size_t totwrite = 0;
    while (node) {
        aof_rwblock * block = node->value;
        if (write(fd, block->buf, block->used) != block->used) {
            return -1;
        }
        totwrite += block->used;
        listDelNode(server.aof_rw_block_list, node);
    }

    return totwrite;
}

void aofRewriteBufferReset() {

    listRelease(server.aof_rw_block_list);
    server.aof_rw_block_list = listCreate();
    listSetFreeMethod(server.aof_rw_block_list, zfree);
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
    c->flag |= CLIENT_FAKE;
    listSetFreeMethod(c->reply, zfree);
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

    if (c->querybuf) {
        zfree(c->querybuf);
    }

    listRelease(c->reply);
    zfree(c);

    debug("freeFakeClient...\r\n");

}

void startLoading(FILE *fp) {
    struct stat st;
    debug("loading aof, start...\r\n");
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
    debug("loading aof, finished...\r\n");
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
        debug("loading aof, open aof file exception...\r\n");
        exit(-1);
    }
    if (st.st_size == 0) {
        debug("loading aof, warning, file size is 0\r\n");
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
        size_t nread;


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
            if (buf[1] == '\0') {
                freeFakeClientArgv(j, argv);
                goto readerr;
            }

            arglen = strtol(buf+1, NULL, 10);
            if (arglen < 1) {
                freeFakeClientArgv(j, argv);
                goto fmterr;
            }

            argsds = sdsnewlen(NULL, arglen);
            nread = fread(argsds, arglen, 1, fp);
            if (nread < 1) {
                sdsfree(argsds);
                freeFakeClientArgv(j, argv);
                goto readerr;
            }

            robj *obj = createObject(REDIS_OBJECT_STRING, argsds);
            argv[j] = obj;
            fseek(fp, 2, SEEK_CUR);
        }

        fake_client->argc = argc;
        fake_client->argv = argv;

        cmd = lookupCommand(fake_client->argv[0]);
        if (cmd == NULL) {
            debug("loading aof, cmd[%s] not found...\r\n", (char *)fake_client->argv[0]->ptr);
            exit(-1);
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
    debug("aof readerr...\r\n");
    if (!feof(fp)) {
        freeFakeClient(fake_client);
        fclose(fp);
        exit(-1);
    }
uxeof:
    debug("aof uxeof...\r\n");
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
    debug("aof fmterr...\r\n");
    freeFakeClient(fake_client);
    fclose(fp);
    exit(-1);
}


void aofUpdateCurrentSize(void) {

    struct stat st;

    if (fstat(server.aof_fd, &st) == -1) {
        debug("aofUpdateCurrentSize fstat failed, fd[%d]...\r\n", server.aof_fd);
        exit(1);
    }

    server.aof_update_size = st.st_size;
}


int rewriteAppendOnlyFileBackground(void) {

    pid_t child_pid;

    if (server.aof_child_pid != -1) {
        return C_ERR;
    }

    if (aofCreatePipes()==C_ERR) {
        return C_ERR;
    }

    if ((child_pid = fork()) > 0) { // child process
        closeListeningSockets();
        char temp_file[128];
        server.aof_child_diff = sdsempty();
        snprintf(temp_file, sizeof(temp_file), "temp_aof_rewrite_bg_%d.aof", child_pid);
        if (rewriteAppendOnlyFileChild(temp_file) == C_OK) {
            exitFromChild(0);
        }
        exitFromChild(1);

    } else if (child_pid == -1) { // fork failed
        aofClosePipes();
        return C_ERR;
    } else { // parent process
        server.aof_child_pid = child_pid;
        server.aof_seldb = -1;
        dictDisableResize();
    }

    return C_OK;
}

void pipeFromChildReadable(struct eventLoop *el, int fd, int mask, void *clientData) {

    char buf[128];

    if (read(fd, buf, sizeof(buf)) == 1 && buf[0] == '!') {
        server.aof_stop_sending_diff = 1;
        if (write(server.aof_pipe_write_ack_to_child, "!", 1) != 1) {
            // todo logger error
        }
    }

    elDeleteFileEvent(el, fd, mask);

}

void aofRewriteBufferPipeWritable(struct eventLoop *el, int fd, int mask, void *clientData) {

    listNode *ln;
    aof_rwblock *rwblock;

    if (!(mask & EL_WRITABLE)) {
        return;
    }

    while (1) {
        ssize_t nwritten;

        ln = listFirst(server.aof_rw_block_list);

        if (!ln || server.aof_stop_sending_diff) {
            elDeleteFileEvent(el, fd, EL_WRITABLE);
            return;
        }

        rwblock = ln->value;
        while (1) {

            if ((nwritten = write(fd, rwblock->buf, rwblock->used)) <= 0) {
                return;
            }

            if (nwritten == rwblock->used) {
                listDelNode(server.aof_rw_block_list, ln);
                break;
            } else {
                memmove(rwblock->buf, rwblock->buf+nwritten, rwblock->used+rwblock->free-nwritten);
                rwblock->used-=nwritten;
                rwblock->free+=nwritten;
            }
        }
    }

}

int aofCreatePipes(void) {

    int j;
    int fds[] = {-1, -1, -1, -1, -1, -1};

    for (j=0; j< sizeof(fds)/2; j++) {
        if (pipe(fds+j) == -1)
            goto cleanup;
    }

    anetNonBlock(fds[0]);
    anetNonBlock(fds[1]);

    elCreateFileEvent(server.el, fds[2], EL_READABLE, pipeFromChildReadable, NULL);

    server.aof_pipe_read_data_from_parent = fds[0];
    server.aof_pipe_write_data_to_child = fds[1];
    server.aof_pipe_read_ack_from_child = fds[2];
    server.aof_pipe_write_ack_to_parent = fds[3];
    server.aof_pipe_read_ack_from_parent = fds[4];
    server.aof_pipe_write_ack_to_child = fds[5];

    server.aof_stop_sending_diff = 0;
    return C_OK;

cleanup:
    for (j=0; j< sizeof(fds)/2; j++) {
        if (fds[j] != -1)
            close(fds[j]);
    }
    return C_ERR;
}

void aofClosePipes(void) {

    elDeleteFileEvent(server.el, server.aof_pipe_write_data_to_child, EL_READABLE);

    server.aof_pipe_read_data_from_parent = -1;
    server.aof_pipe_write_data_to_child = -1;
    server.aof_pipe_read_ack_from_child = -1;
    server.aof_pipe_write_ack_to_parent = -1;
    server.aof_pipe_read_ack_from_parent = -1;
    server.aof_pipe_write_ack_to_child = -1;
}

int rewriteAppendOnlyFileChild(char *name) {

    char buf[128];
    FILE *fp;

    char *filename = "temp_aof_rewrite_%d.aof";
    snprintf(buf, sizeof(buf), filename, getpid());

    fp = fopen(buf, "w");
    if (fp == NULL) goto err;

    if (rewriteAppendOnlyFile(fp) == C_ERR)
        goto err;

    fclose(fp);
    fp = NULL;

    if (rename(filename, name) == -1)
        goto err;

    return C_OK;

err:
    if (fp != NULL) {
        fclose(fp);
        fp = NULL;
    }

    unlink(filename);
    return C_ERR;

}

int rewriteAppendOnlyFile(FILE *fp) {

    int j;
    sds buf;
    size_t nwriten;
    mstime_t start;
    int nodata;
    char read_ack_from_parent[128];

    buf = sdsempty();
    for (j=0; j<server.dbnum; j++) {
        dictIter iter;
        redisDb db;
        dictEntry *de;
        size_t db_num_size;
        robj *key, *value;
        long long expire;

        db = server.dbs[j];
        dictSafeGetIterator(db.dict, &iter);
        char db_num[128];
        db_num_size = ll2string(db_num, (long long)j);
        buf = sdscatfmt(buf, "*2\r\n$6\r\nSELECT\r\n$%u\r\n", j, db_num_size);
        while ((de=dictNext(&iter)) != NULL) {
            key = de->key;
            value = de->value.ptr;
            expire = getExpire(&db, key);
            if (expire != -1 && expire < mstime()) continue;

            if (value->type == REDIS_OBJECT_STRING) {

                buf = sdscatprintf(buf, "*3\r\n$3\r\nSET\r\n");
                buf = sdscatfmt(buf, "$%u\r\n", sdslen(key->ptr));
                buf = sdscatsds(buf, key->ptr);
                buf = sdscatsds(buf, shared.crlf->ptr);

                buf = sdscatfmt(buf, "$%u\r\n", sdslen(value->ptr));
                buf = sdscatsds(buf, value->ptr);
                buf = sdscatsds(buf, shared.crlf->ptr);

            } else if (value->type == REDIS_OBJECT_LIST) {

            } else { // todo more object type is coming in future.

            }

            if (expire != -1) {

                buf = sdscatprintf(buf, "*2\r\n$9\r\nPEXPIREAT\r\n");
                buf = sdscatfmt(buf, "$%u\r\n", sdslen(key->ptr));
                buf = sdscatsds(buf, key->ptr);
                buf = sdscatsds(buf, shared.crlf->ptr);

                buf = sdscatfmt(buf, "$%u\r\n", sdslen(value->ptr));
                buf = sdscatfmt(buf, "%U", mstime() + expire);
                buf = sdscatsds(buf, shared.crlf->ptr);
            }

            if (sdslen(buf) >= AOF_FWRITE_BLOCK_SIZE ) {
                if ((nwriten = fwrite(buf, AOF_FWRITE_BLOCK_SIZE, 1, fp)) == 0) {
                    if (errno == EAGAIN) continue;
                    goto err;
                }
            }

            if (j % 100 == 0) {
                readDiffFromParent();
            }
        }
    }

    if (sdslen(buf) >= AOF_FWRITE_BLOCK_SIZE ) {
        if ((nwriten = fwrite(buf, AOF_FWRITE_BLOCK_SIZE, 1, fp)) == 0) {
            goto err;
        }
    }

    start = mstime();
    nodata = 0;

    while (mstime() - start < 1000 && nodata < 20) {

        if (elWait(server.aof_pipe_read_data_from_parent, EL_READABLE, 1) <= 0) {
            nodata++;
            continue;
        }

        nodata = 0;
        readDiffFromParent();
    }

    if (sendParentStopWriteAppendDiff() == C_ERR)
        goto err;

    anetNonBlock(server.aof_pipe_read_ack_from_parent);

    if (syncRead(server.aof_pipe_read_ack_from_parent, read_ack_from_parent,
                 sizeof(read_ack_from_parent), 5000) != 1 || read_ack_from_parent[0] != '!') {
        goto err;
    }

    readDiffFromParent();

    fflush(fp);
    fsync(fileno(fp));
    sdsfree(buf);
    return C_OK;

err:
    sdsfree(buf);
    return C_ERR;



}

size_t readDiffFromParent(void) {

    char buf[65535];
    size_t nread;
    nread = read(server.aof_pipe_read_data_from_parent, buf, sizeof(buf));
    if (nread > 0) {
        sds s = sdsnewlen(buf, nread);
        server.aof_child_diff = sdscatsds(server.aof_child_diff, s);
        sdsfree(s);
    }
    return nread;
}

int sendParentStopWriteAppendDiff(void) {

    if (write(server.aof_pipe_write_ack_to_parent, "!", 1) == -1) {
        return C_ERR;
    }

    return C_OK;
}

void aofRewriteDoneHandler(int bysignal, int code) {

    int newfd = -1;

    if (!bysignal && code == 0) {

        if (server.aof_child_pid == -1)
            goto cleanup;

        char tempfile[255];
        snprintf(tempfile, 255, "temp_aof_rewrite_%d.aof", server.aof_child_pid);

        newfd = open(tempfile, O_WRONLY | O_APPEND);
        if (newfd == -1) {
            unlink(tempfile);
            goto cleanup;
        }

        if (aofRewriteBufferWrite(newfd) == -1) {
            unlink(tempfile);
            goto cleanup;
        }

        if (rename(tempfile, server.aof_filename) == -1) {
            unlink(tempfile);
            goto cleanup;
        }

        if (server.aof_fsync == AOF_FSYNC_ALWAYS) {
            fsync(newfd);
        } else if (server.aof_fsync == AOF_FSYNC_EVERYSEC) {
            bioCreateBackgroundJob(BIO_AOF_FSYNC, newfd);
        }

        if (server.aof_fd == -1) {
            goto cleanup;
        } else {
            int oldfd = server.aof_fd;
            server.aof_fd = newfd;
            bioCreateBackgroundJob(BIO_CLOSE_FILE, oldfd);
            newfd = -1;
        }
    }


cleanup:
    if (newfd != -1)
        close(newfd);

    aofClosePipes();
    aofRewriteBufferReset();
    server.aof_child_pid = -1;
    sdsfree(server.aof_buf);
    server.aof_buf = sdsempty();
}
