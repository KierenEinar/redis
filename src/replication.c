//
// Created by kieren jiang on 2023/9/25.
//
#include "server.h"

// ------------------------- slave ----------------------

#define SYNC_CMD_READ (1<<0)
#define SYNC_CMD_WRITE (1<<1)
sds sendSynchronousCommand(int flags, int fd, ...) {

    if (flags & SYNC_CMD_WRITE) {

        sds cmd = sdsempty();
        va_list args;
        va_start(args, fd);
        while (1) {
            char *argv = va_arg(args, char *);
            if (argv == NULL) break;
            if (sdslen(cmd)) cmd = sdscatlen(cmd, " ", 1);
            cmd = sdscatlen(cmd, argv, strlen(argv));
        }
        va_end(args);

        cmd = sdscatlen(cmd, "\r\n", 2);
        if (syncWrite(fd, cmd, sdslen(cmd), server.repl_send_timeout*1000) == -1) {
            debug("sendSynchronousCommand cmd=%s, failed\n", cmd);
            sdsfree(cmd);
            return sdscatprintf(sdsempty(), "-Writing to master:%s\r\n", strerror(errno));
        }

        sdsfree(cmd);

    }

    if (flags & SYNC_CMD_READ) {

        char tmp[1024];
        size_t nread;
        if ((nread = syncReadLine(fd, tmp, sizeof(tmp), server.repl_read_timeout*1000)) == -1) {
            return sdscatprintf(sdsempty(), "-Reading from master:%s\r\n", strerror(errno));
        }
        return sdsnewlen(tmp, nread);
    }


    return NULL;
}

void replicationDiscardCacheMaster() {

    if (!server.cache_master) return;
    client *c = server.cache_master;
    server.cache_master = NULL;
    freeClient(c);
}

int slaveTryPartialResynchronization(int fd, int read_reply) {

    char *psync_replid;
    char psync_offset[32];
    sds reply;

    if (!read_reply) {

        if (server.cache_master) {
            psync_replid = server.cache_master->replid;
            snprintf(psync_offset, sizeof(psync_offset), "%lld", server.cache_master->repl_offset+1);
        } else {
            psync_replid = "?";
            memcpy(psync_offset, "-1", 3);
        }

        if ((reply = sendSynchronousCommand(SYNC_CMD_WRITE, fd, "PSYNC2", psync_replid, psync_offset, NULL)) != NULL) {
            sdsfree(reply);
            return PSYNC_WRITE_ERROR;
        }

        return PSYNC_WAIT_REPLY;
    }

    reply = sendSynchronousCommand(SYNC_CMD_READ, fd, NULL);

    if (sdslen(reply) == 0) {
        sdsfree(reply);
        return PSYNC_WAIT_REPLY;
    }

    elDeleteFileEvent(server.el, fd, EL_READABLE);

    if (!strncmp(reply, "+FULLRESYNC", 11)) {

        char *replid, *offset;

        // +FULLRESYNC RUNID OFFSET
        replid = strchr(reply, ' ');
        if (replid) {
            replid++;
            offset = strchr(replid, ' ');
            if (offset) {
                offset++;
            }
        }

        if (!replid || !offset || strlen(replid) != CONFIG_REPL_RUNID_LEN) {
            sdsfree(reply);
            return PSYNC_READ_ERROR;
        }

        memcpy(server.master_replid, replid, CONFIG_REPL_RUNID_LEN);
        server.master_replid[CONFIG_REPL_RUNID_LEN] = '\0';
        server.master_initial_offset = strtoll(offset, NULL, 10);
        sdsfree(reply);
        replicationDiscardCacheMaster();
        return PSYNC_FULL_RESYNC;
    }

    return PSYNC_NOT_SUPPORT;

}


void syncWithMaster(struct eventLoop *el, int fd, int mask, void *clientData) {

    sds err, reply;
    int max_retry, dfd, psync_result;
    char tmpfile[256];

    max_retry = 5;
    dfd = -1;
    // todo check the socket


    if (server.repl_state == REPL_STATE_CONNECTING) {

        elDeleteFileEvent(el, fd, EL_WRITABLE);
        if ((err=sendSynchronousCommand(SYNC_CMD_WRITE, fd, "PING", NULL)) != NULL) {
            sdsfree(err);
            goto write_error;
        }
        server.repl_state = REPL_STATE_RECEIVE_PONG;
        return;
    }

    if (server.repl_state == REPL_STATE_RECEIVE_PONG) {

        if ((reply = sendSynchronousCommand(SYNC_CMD_READ, fd)) != NULL) {
            if (reply[0] != '+') {
                debug("slave ping, master reply failed\n");
                sdsfree(reply);
                goto error;
            }
        }
        sdsfree(reply);
        server.repl_state = REPL_STATE_SEND_AUTH;
        return;
    }


    if (server.repl_state == REPL_STATE_SEND_AUTH) {

        if (server.master_auth) {
            if ((err = sendSynchronousCommand(SYNC_CMD_WRITE, fd, "AUTH", server.master_auth, NULL)) != NULL) {
                debug("slave send auth failed, err=%s\n", strerror(errno));
                sdsfree(err);
                goto write_error;
            }
        }

        server.repl_state = REPL_STATE_RECEIVE_AUTH;
        return;
    }

    if (server.repl_state == REPL_STATE_RECEIVE_AUTH) {

        if ((reply = sendSynchronousCommand(SYNC_CMD_READ, fd,  NULL)) != NULL) {
            if (reply[0] == '-') {
                debug("slave auth, master reply failed, err=%s\n", reply);
                sdsfree(reply);
                goto error;
            }
            sdsfree(reply);
        }

        server.repl_state = REPL_STATE_SEND_CAPA;
        return;
    }

    if (server.repl_state == REPL_STATE_SEND_CAPA) {

        if ((err = sendSynchronousCommand(SYNC_CMD_WRITE, fd, "REPLCONF",
                                          "capa", "eof", "capa", "psync2")) != NULL) {
            debug("slave send capa failed, err=%s\n", err);
            sdsfree(err);
            goto write_error;
        }
        server.repl_state = REPL_STATE_RECEIVE_CAPA;
        return;
    }


    if (server.repl_state == REPL_STATE_RECEIVE_CAPA) {

        if ((reply = sendSynchronousCommand(SYNC_CMD_READ, fd,  NULL)) != NULL) {
            if (reply[0] == '-') {
                debug("warming(non critical), slave capa, master do not understand, reply=%s\n", reply);
            }
            sdsfree(reply);
        }

        server.repl_state = REPL_STATE_SEND_PSYNC;
        return;
    }

    if (server.repl_state == REPL_STATE_SEND_PSYNC) {

       if (slaveTryPartialResynchronization(fd, 0) == PSYNC_WRITE_ERROR) {
           goto write_error;
       }

       server.repl_state = REPL_STATE_RECEIVE_PSYNC;
       return;
    }

    if (server.repl_state != REPL_STATE_RECEIVE_PSYNC) {
        goto error;
    }

    psync_result = slaveTryPartialResynchronization(fd, 1);

    if (psync_result == PSYNC_WAIT_REPLY) {
        return;
    }

    if (psync_result == PSYNC_FULL_RESYNC) {

        // force disconnect slaves
        // create new backlog buffer

        while (max_retry--) {
            snprintf(tmpfile, 256, "tmp-%d-appendonly.aof", (int)server.unix_time);
            dfd = open(tmpfile, O_CREAT | O_WRONLY);
            if (dfd != -1) break;
            sleep(1);
        }

        if (dfd == -1) {
            debug("Opening the temp file needed for MASTER <-> SLAVE synchronization: %s",strerror(errno));
            goto error;
        }

        if (elCreateFileEvent(server.el, fd, EL_READABLE, readSyncBulkPayload, NULL) == -1) {
            debug("elCreateFileEvent syncReadBulkPayload needed for MASTER <-> SLAVE synchronization");
            goto error;
        }

        anetNonBlock(dfd);
        server.repl_state = REPL_STATE_TRANSFER;
        server.repl_transfer_tmp_fd = dfd;
        server.repl_transfer_size = -1l;
        server.repl_transfer_nread = 0;
        memcpy(server.repl_transfer_tmp_file, tmpfile, strlen(tmpfile)+1);

        return;

    }


error:

write_error:


    return;
}


int connectWithMaster(void) {

    int fd;

    fd = anetTcpConnect(server.master_host, server.master_port, NULL, O_NONBLOCK);
    if (fd == -1)
        return C_ERR;

    if (elCreateFileEvent(server.el, fd, EL_WRITABLE | EL_READABLE, syncWithMaster, NULL) == -1) {
        close(fd);
        return C_ERR;
    }

    server.repl_state = REPL_STATE_CONNECTING;
    server.repl_transfer_s = fd;
    return C_OK;
}

void replicationCreateMaterClient(int fd) {
    server.master = createClient(fd);
    server.master->flag |= CLIENT_MASTER;
    server.master->repl_offset = server.master_initial_offset;
    memcpy(server.master->replid, server.master_replid, CONFIG_REPL_RUNID_LEN);
    server.master->replid[CONFIG_REPL_RUNID_LEN] = '\0';
}

int replicationIsInHandshake(void) {
    return server.repl_state >= REPL_STATE_RECEIVE_PONG &&
        server.repl_state <= REPL_STATE_RECEIVE_PSYNC;
}

void undoConnectWithMaster(void){
    elDeleteFileEvent(server.el, server.repl_transfer_s, EL_WRITABLE | EL_READABLE);
    close(server.repl_transfer_s);
    server.repl_transfer_s = -1;

    server.master_initial_offset = -1;
    memcpy(server.master_replid, 0, sizeof(server.master_replid));
    server.repl_state = REPL_STATE_CONNECT;
}

void replicationAbortSyncTransfer(void) {

    undoConnectWithMaster();
    close(server.repl_transfer_tmp_fd);
    server.repl_transfer_tmp_fd = -1;
    unlink(server.repl_transfer_tmp_file);
    memcpy(server.repl_transfer_tmp_file, 0, sizeof(server.repl_transfer_tmp_file));
}


void cancelReplicationHandShake() {

    if (server.repl_state == REPL_STATE_TRANSFER) {
        replicationAbortSyncTransfer();
    } else if (server.repl_state == REPL_STATE_CONNECTING ||
            replicationIsInHandshake()) {
        undoConnectWithMaster();
    }

}

void readSyncBulkPayload(struct eventLoop *el, int fd, int mask, void *clientData) {


    char buf[4096];
    int eof_reached;
    int old_aof_state;
    size_t nread;

    static char eofmark[CONFIG_REPL_RUNID_LEN];
    static char lastbytes[CONFIG_REPL_RUNID_LEN];

    if (server.repl_transfer_size == -1) {

        if ((nread = syncReadLine(fd, buf, 1024, server.repl_read_timeout*1000)) == -1) {
            debug("<REPLICATE PSYNC> I/O error reading bulk count from MASTER: %s", strerror(errno));
            goto error;
        }

        // $EOF:<40bytes>
        if (buf[0] == '-') {
            debug("<REPLICATE PSYNC> MASTER abort replication, error msg: %s", buf+1);
            goto error;
        }

        if (buf[0] == '\0') {
            // at this stage, master used new lines to work as PING to keepalive the connections.
            // so we ignore it.
            return;
        }

        if (buf[0] != '$') {
            debug("<REPLICATE PSYNC> SLAVE bad protocol received: %s, are you sure the host and port is master?", buf+1);
            goto error;
        }

        if (!strncmp(buf+1, "EOF:", 4) && strlen(buf+5) >= CONFIG_REPL_RUNID_LEN) {
            memcpy(eofmark, buf+5, CONFIG_REPL_RUNID_LEN);
            memcpy(lastbytes, 0, CONFIG_REPL_RUNID_LEN);
            server.repl_transfer_size = 0; // use 0 to mark is eof capa.
            return;
        }

        debug("<REPLICATE PSYNC> SLAVE protocol we are not support, are you sure the master version right?");
        goto error;

    }

    eof_reached = 0;
    nread = read(fd, buf, sizeof(buf));

    if (nread <= 0) {
        debug("<REPLICATE PSYNC> SLAVE TRANSFER I/O trying to sync with master: %s",
              nread == -1 ? strerror(errno) : "lost connection");
        goto error;
    }

    // update the lastbytes
    if (nread >= CONFIG_REPL_RUNID_LEN) {
        memcpy(lastbytes, buf+(nread-CONFIG_REPL_RUNID_LEN), CONFIG_REPL_RUNID_LEN);
    } else {
        memmove(lastbytes, lastbytes+nread, CONFIG_REPL_RUNID_LEN-nread);
        memcpy(lastbytes+(CONFIG_REPL_RUNID_LEN-nread), buf, nread);
    }

    if (memcmp(lastbytes, eofmark, CONFIG_REPL_RUNID_LEN) == 0) {
        eof_reached = 1;
    }

    if (write(server.repl_transfer_tmp_fd, buf, nread) != nread) {
        debug("<REPLICATE PSYNC> SLAVE TRANSFER write buf to disk failed, err:%s", strerror(errno));
        goto  error;
    }

    server.repl_transfer_nread += nread;

    if (eof_reached) {

        if (ftruncate(server.repl_transfer_tmp_fd, (off_t)server.repl_transfer_nread-CONFIG_REPL_RUNID_LEN) == -1) {
            debug("<REPLICATE PSYNC> SLAVE ftruncate EOF fd:%d, file_name:%s, failed:%s",
                  server.repl_transfer_tmp_fd, server.repl_transfer_tmp_file, strerror(errno));
            goto error;
        }

        old_aof_state = server.aof_state;
        if (old_aof_state != AOF_OFF) stopAppendOnly();

        // rename aof.
        if (rename(server.repl_transfer_tmp_file, server.aof_filename) == -1) {
            debug("<REPLICATE PSYNC> SLAVE rename  file_name:%s to aof file failed:%s",
                  server.repl_transfer_tmp_file, strerror(errno));
            goto error;
        }

        // clear whole dbs.
        emptyDb(-1);

        elDeleteFileEvent(server.el, server.repl_transfer_s, EL_READABLE);

        if (loadAppendOnlyFile(server.aof_filename) == C_ERR) {
            debug("<REPLICATE PSYNC> SLAVE load append only file from MASTER failed");
            goto error;
        }

        debug("<REPLICATE PSYNC> SLAVE load append only file from MASTER completed");
        server.repl_state = REPL_STATE_CONNECTED;
        memcpy(server.repl_transfer_tmp_file, 0, sizeof(server.repl_transfer_tmp_file));
        replicationCreateMaterClient(server.repl_transfer_s);
        close(server.repl_transfer_tmp_fd);
        server.repl_transfer_tmp_fd = -1;
        server.repl_transfer_s = -1;
        if (old_aof_state != AOF_OFF) restartAOF();
    }

    return;


error:
    cancelReplicationHandShake();

    return;
}

void restartAOF() {
    int max_retry;
    max_retry = 10;
    while (max_retry--) {
        if (startAppendOnly() == C_OK) {
            return;
        }
        debug("Failed enabling the AOF after successful master synchronization! Trying it again in one second.");
        sleep(1);
    }

    debug("FATAL: this slave instance finished the synchronization with its master, but the AOF can't be turned on. Exiting now.");
    exit(1);
}