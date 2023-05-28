//
// Created by kieren jiang on 2023/5/6.
//

#include "server.h"

typedef struct aofrwblock {
    int free;
    int used;
    char buf[AOF_REWRITE_BLOCK_SIZE];
} aofrwblock;

static aofrwblock* initblock() {
    aofrwblock *block = zmalloc(sizeof(aofrwblock));
    block->free = AOF_REWRITE_BLOCK_SIZE;
    block->used = 0;
    return block;
}

void aofChildWriteDiff() {

    while (1) {

        listNode *node = listFirst(server.aof_rewrite_diff_block);
        if (!node || server.aof_stop_send_diff) {
            elDeleteFileEvent(server.el, server.aof_rewrite_writediff_to_child, EL_WRITABLE);
            return;
        }

        aofrwblock *block = node->value;
        size_t nwriten =  write(server.aof_rewrite_writediff_to_child, block->buf, block->used);
        if (nwriten <= 0)
            return;

        memmove(block->buf, block->buf+nwriten, nwriten);
        block->used-=nwriten;
        block->free+=nwriten;
        if (block->used == 0) {
            listDelNode(server.aof_rewrite_diff_block, node);
            // set free method in listfreemethod, no need to free
        }
    }

}


void aofRewriteBufferAppend(char *s, size_t len) {

    while (len) {
        aofrwblock *block;
        listNode *node = listLast(server.aof_rewrite_diff_block);
        if (node) {
            block = node->value;
            if (block->free > 0) {
                size_t nwriten = len > block->free ? block->free : len;
                memcpy(server.aof_rewrite_diff_block, s, nwriten);
                block->free-=nwriten;
                block->used+=nwriten;
                len-=nwriten;
            }
        }

        if (len) {
            block = initblock();
            size_t nwriten = len > block->free ? block->free : len;
            memcpy(server.aof_rewrite_diff_block, s, nwriten);
            block->free-=nwriten;
            block->used+=nwriten;
            len-=nwriten;
            listAddNodeTail(server.aof_rewrite_diff_block, block);
        }
    }


    if (!elGetFileEvent(server.el, server.aof_rewrite_writediff_to_child, EL_WRITABLE))
        elCreateFileEvent(server.el, server.aof_rewrite_writediff_to_child, EL_WRITABLE,
                          aofChildWriteDiff, NULL);

}

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
        when = mstime() + when;
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

        server.aof_last_write_status = C_ERR;

        if (server.aof_fsync == AOF_FSYNC_ALWAYS) {
            exit(1);
        }

        return;

    }

    if (server.aof_last_write_status == C_ERR)
        server.aof_last_write_status = C_OK;

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


//------------aof rewrite background -----------------

size_t aofReadDiffFromParent() {

    char buffer[65535];
    ssize_t nread, totread = 0;
    while (1) {
        nread = read(server.aof_rewrite_readdiff_from_parent, buffer, sizeof(buffer));
        if (nread <= 0) {
            return totread;
        }
        totread+=nread;
        server.aof_child_diff = sdscatlen(server.aof_child_diff, buffer, nread);
    }
}

// trigger when child send "ack" to parent
void aofChildPipeReadable(eventLoop *el, int fd, int mask, void *clientData) {

    char byte;

    if (read(fd, &byte, 1) == 1 && byte == '!') {

        server.aof_stop_send_diff = 1;
        fdSetNonBlock(server.aof_rewrite_writeack_to_child);
        if (write(server.aof_rewrite_writeack_to_child, "!", 1) <= 0) {
            // todo server log error
            // when write failed, child will wait until timeout and ternimate
        }
    }

    elDeleteFileEvent(el, fd, EL_READABLE);
}

int createPipes(void) {

    int fds[] = {-1, -1, -1, -1, -1, -1};

    if (pipe(fds) == -1) goto cerr;
    if (pipe(fds+2) == -1) goto cerr;
    if (pipe(fds+4) == -1) goto cerr;

    if (fdSetNonBlock(fds[0]) == C_ERR) goto cerr;
    if (fdSetNonBlock(fds[1]) == C_ERR) goto cerr;

    server.aof_rewrite_readdiff_from_parent = fds[0];
    server.aof_rewrite_writediff_to_child = fds[1];
    server.aof_rewrite_readack_from_child = fds[2];
    server.aof_rewrite_writeack_to_parent = fds[3];
    server.aof_rewrite_readack_from_parent = fds[4];
    server.aof_rewrite_writeack_to_child = fds[5];

    server.aof_stop_send_diff = -1;
    elCreateFileEvent(server.el, fds[2], EL_READABLE, aofChildPipeReadable, NULL);
    return C_OK;


cerr:
    for (int i=0; i< sizeof(fds); i++) if (fds[i] > 0) close(fds[i]);
    return C_ERR;
}

void aofClosePipes() {

    elDeleteFileEvent(server.el, server.aof_rewrite_writediff_to_child, EL_WRITABLE);
    elDeleteFileEvent(server.el, server.aof_rewrite_readack_from_parent, EL_READABLE);

    close(server.aof_rewrite_writediff_to_child);
    close(server.aof_rewrite_readdiff_from_parent);
    close(server.aof_rewrite_readack_from_parent);
    close(server.aof_rewrite_writeack_to_child);
    close(server.aof_rewrite_readack_from_child);
    close(server.aof_rewrite_writeack_to_parent);

}


int rewriteAppendOnlyFileRio(rio *r) {

    int j;
    ssize_t processed;
    dictEntry *de;
    dictIterator *di;
    mstime_t now = mstime();
    for (j=0; j<server.db_nums; j++) {

        db *db = server.db + j;

        if (dictSize(db->dict) == 0) continue;

        char selectedcmd[] = "*2\r\n$6\r\nSELECT\r\n";

        if (rioWrite(r, selectedcmd, sizeof(selectedcmd) - 1) == 0)
            return C_ERR;

        if (rioWriteBulkLongLong(r, (long long)j) == 0)
            return C_ERR;

        di = dictGetSafeIterator(db->dict);

        while ((de = dictNext(di)) != NULL) {

            robj key, *o;
            sds kstr = de->key;
            long long expire;
            o = dictFetchValue(db, kstr);

            initStaticStringObject(&key, kstr);

            expire = getExpire(db, &key);

            // ignore the expire key
            if (expire < now && expire != -1) continue;

            if (o->type == OBJECT_STRING) {
                char *cmd = "*3\r\n$3\r\nSET\r\n";
                if (rioWrite(r, cmd, sizeof(cmd)-1) == 0) goto err;
                if (rioWriteBulkObject(r, &key) == 0) goto err;
                if (rioWriteBulkObject(r, o) == 0) goto err;
            } else { // todo handle the rest type

            }

            if (expire != -1) { // expireat

                char cmd[] = "*3\r\n$9\r\nPEXPIREAT\r\n";
                if (rioWrite(r, cmd, sizeof(cmd) - 1) == -1) goto err;
                if (rioWriteBulkString(r, kstr, sdslen(kstr)) == -1) goto err;
                if (rioWriteBulkLongLong(r, expire) == -1) goto err;
            }

            // processed read diff from parent
            if (r->processed_bytes >= processed + AOF_READDIFF_PER_INTERVAL_BYTES) {
                processed = r->processed_bytes;
                aofReadDiffFromParent();
            }

        }

        dictReleaseIterator(di);
    }

    return C_OK;

err:
    dictReleaseIterator(di);
    return C_ERR;
}

int rewriteAppendOnlyFile(char *filename) {

    rio aof;
    char tmp[256];
    snprintf(tmp, "temp-aof-rewrite-%s", getpid());
    FILE *fp = fopen(tmp, "w");
    if (fp == NULL) return C_ERR;

    rioInitWithFile(&aof, fp);

    if (server.aof_rewrite_incremental_fsync) {
        rioSetAutoSync(&aof, server.aof_rewrite_incremental_fsync);
    }

    server.aof_child_diff = sdsempty();

    if (rewriteAppendOnlyFileRio(&aof) == C_ERR)
        goto err;

    if (fflush(fp) == EOF) goto err;
    fsync(fileno(fp));

    mstime_t now = mstime();
    int nodata = 0;

    while (mstime() - now <= 1000 && nodata <= 20) {
        if (aofReadDiffFromParent() == 0) {
            nodata++;
            continue;
        }
        nodata = 0;
    }

    // send ack to parent to stop write aof_rewrite_buffer_block
    if (write(server.aof_rewrite_writeack_to_parent, 1, '!') == 0) goto err;
    fdSetNonBlock(server.aof_rewrite_readack_from_parent);
    char buf;
    if (syncRead(server.aof_rewrite_readack_from_parent, &buf, 1, 5000) != 1 || buf != '!')
        goto err;

    aofReadDiffFromParent();

    if (rioWrite(&aof, server.aof_child_diff, sdslen(server.aof_child_diff)) == 0)
        goto err;

    if (fflush(fp) == EOF) goto err;
    fsync(fileno(fp));
    if (fclose(fp) == EOF) goto err;
    // rename file ....
    if (rename(tmp, filename) == -1) {
        unlink(tmp);
        return C_ERR;
    }

    return C_OK;
err:
    fclose(fp);
    unlink(tmp);
    return C_ERR;
}


int rewriteAppendOnlyFileBackground(void) {

    pid_t childPid;

    if (server.aof_child_pid != -1)
        return C_ERR;

    if (createPipes() == C_ERR)
        goto werr;

    if ((childPid = fork()) == 0) { // child process

        char tmp[256];
        snprintf(tmp, "temp-aof-rewrite-bg-%d", getpid());
        closeListeningFds();
        if (rewriteAppendOnlyFile(tmp) == C_ERR) {
            exitFromChild(-1);
        } else {
            exitFromChild(0);
        }
    } else {

        if (childPid == -1) {
            aofClosePipes();
            return C_ERR;
        }

        server.aof_child_pid = childPid;

        server.select_db = -1;

    }

werr:



}

