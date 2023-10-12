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

    elDeleteFileEvent(server.el, fd, EL_READABLE | EL_WRITABLE);

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
            tmpfile[strlen(tmpfile)] = '\0';
            dfd = open(tmpfile, O_CREAT | O_WRONLY, 0644);
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

        server.repl_state = REPL_STATE_TRANSFER;
        server.repl_transfer_tmp_fd = dfd;
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

void readSyncBulkPayload(struct eventLoop *el, int fd, int mask, void *clientData) {

}