//
// Created by kieren jiang on 2023/9/25.
//
#include "server.h"


// ------------------------ MASTER ---------------------

void changeReplicationId() {

}

void freeReplicationBacklog() {
    server.repl_backlog_idx = 0;
    server.repl_backlog_histlen = 0;
    server.repl_backlog_off = server.master_repl_offset + 1;
    zfree(server.repl_backlog);
    server.repl_backlog = NULL;
}

void createReplicationBacklog() {
    server.repl_backlog = zmalloc(server.repl_backlog_size);
    server.repl_backlog_idx = 0l;
    server.repl_backlog_histlen = 0l;

    server.repl_backlog_off = server.master_repl_offset + 1;
}

int replicationSetupFullResync(client *slave, off_t offset) {

    char buf[128];
    int buflen;
    slave->repl_state = SLAVE_STATE_WAIT_BGSAVE_END;
    slave->psync_initial_offset = offset;

    buflen = snprintf(buf, sizeof(buf), "+FULLRESYNC %s %lld\r\n",
                      server.master_replid,
                      offset);

    if (write(slave->fd, buf, buflen) != buflen) {
        freeClient(slave);
        return C_ERR;
    }

    return C_OK;

}

int startBgAofSaveForSlaveSockets() {

    listIter li;
    listNode *ln;
    client *slave;
    pid_t childpid;
    int pipefds[2], numfds, retval;
    int *fds, *states;
    unsigned long long *clientids;


    if (server.aof_child_pid != -1) return C_ERR;
    if (pipe(pipefds) == -1) {
        debug("BgAofForSlaveSockets pipe system call, err=%s", strerror(errno));
        return C_ERR;
    }

    numfds = 0;
    fds = zmalloc(sizeof(int) * listLength(server.slaves));
    clientids = zmalloc(sizeof(unsigned long long) * listLength(server.slaves));

    listRewind(server.slaves, &li);
    while ((ln = listNext(&li)) != NULL) {
        slave = ln->value;
        if (slave->repl_state == SLAVE_STATE_WAIT_BGSAVE_START) {
            if (replicationSetupFullResync(slave, server.master_repl_offset) == C_OK) {
                clientids[numfds] = slave->id;
                fds[numfds++] = slave->fd;
                anetBlock(slave->fd);
                anetTcpSendTimeout(slave->fd, server.repl_slave_send_timeout * 1000);
                // anetTcpNoDelay(slave->fd);
            }
        }
    }

    server.aof_repl_read_from_child = pipefds[0];
    server.aof_repl_write_to_parent = pipefds[1];

    if ((childpid = fork()) == 0) { // spawn child process.

        states = zmalloc(sizeof(int)*numfds);
        memset(states, 0, numfds);

        closeListeningSockets();
        retval = aofSaveToSlavesWithEOFMark(fds, states, numfds);

        if (retval == C_OK) {

            void *msg = zmalloc(sizeof(uint64_t*) * (1 + numfds * 2));
            size_t msglen = sizeof(uint64_t*) * (1 + numfds * 2);
            uint64_t *len = msg;
            uint64_t *ids = len + 1;
            *len = numfds;

            for (int j = 0; j<numfds; j++) {
                *ids++ = clientids[j];
                *ids++ = states[j];
            }

            if (write(server.aof_repl_write_to_parent, msg, msglen) != msglen) {
                retval = C_ERR;
            }

            zfree(msg);
        }

        zfree(states);
        zfree(fds);
        zfree(clientids);
        exitFromChild(retval == C_OK ? 0 : 1);

    } else {

        if (childpid == -1) {

            listRewind(server.slaves, &li);
            while ((ln = listNext(&li)) != NULL) {
                slave = ln->value;
                for (int i=0; i<numfds; i++) {
                    if (slave->id == clientids[i]) {
                        slave->repl_state = SLAVE_STATE_WAIT_BGSAVE_START;
                        slave->psync_initial_offset = -1;
                        break;
                    }
                }
            }

            close(pipefds[0]);
            close(pipefds[1]);
            server.aof_repl_read_from_child = -1;
            server.aof_repl_write_to_parent = -1;

        } else {

            // child fork success.
            server.aof_child_pid = childpid;
            server.aof_save_type = AOF_SAVE_TYPE_REPLICATE_SOCKET;
        }

        zfree(fds);
        zfree(clientids);

    }

    return childpid != -1 ? C_OK : C_ERR;

}

int startBgAofSaveForReplication(int mincapa) {

    int socket_type = server.repl_diskless_sync && (mincapa & REPL_CAPA_EOF);
    int retval;

    if (socket_type) {
        retval = startBgAofSaveForSlaveSockets();
    } else {
        // todo start bg aof disks for slaves
        retval = C_ERR;
    }

    if (retval == C_ERR) {

        listIter li;
        listNode *ln;
        client *slave;

        listRewind(server.slaves, &li);

        while ((ln = listNext(&li)) != NULL) {
            slave = listNodeValue(ln);
            if (slave->repl_state == SLAVE_STATE_WAIT_BGSAVE_END) {
                slave->flag &= ~CLIENT_SLAVE;
                listDelNode(server.slaves, ln);
                addReplyError(slave, "Slave bg save failed, stop sync continue.");
                slave->flag |= CLIENT_CLOSE_AFTER_REPLY;
            }
        }

        return C_ERR;

    }

    return C_OK;
}

void putSlaveOnline(client *slave) {

    slave->repl_put_online_ack = 0;
    slave->repl_last_ack = server.unix_time;
    anetNonBlock(slave->fd);

    if (elCreateFileEvent(server.el, slave->fd, EL_WRITABLE,
                          sendClientData, NULL) == EL_ERR) {
        debug("Unable to register writable event for slave bulk transfer: %s", strerror(errno));
        freeClient(slave);
    }
}


void replicationFeedSlaves(int dbid, robj **argv, int argc) {

    listNode *ln;
    listIter li;
    client *slave;

    if (server.master_host) return;

    if (listLength(server.slaves) == 0 && server.repl_backlog == NULL) return;

    if (server.repl_seldbid != dbid) {

        robj *selcmd;

        if (dbid >= 0 && dbid < OBJ_SHARED_COMMAND_SIZE) {
            selcmd = shared.commands[dbid];
        } else {

            char ll2str[21];
            size_t llen = ll2string(ll2str, dbid);
            sds s = sdscatprintf(sdsempty(), "*2\r\n$6\r\nSELECT\r\n$%d\r\n%d\r\n", llen, dbid);
            selcmd = createStringObject(s, sdslen(s));
        }

        listRewind(server.slaves, &li);

        while ((ln = listNext(&li)) != NULL) {

            slave = listNodeValue(ln);
            if (slave->repl_state == SLAVE_STATE_WAIT_BGSAVE_START) continue;
            addReply(slave, selcmd);
        }

        if (server.repl_backlog) {
            feedReplicationBacklogObject(selcmd);
        }

        if (dbid < 0 || dbid >= OBJ_SHARED_COMMAND_SIZE) decrRefCount(selcmd);

    }

    server.repl_seldbid = dbid;

    if (server.repl_backlog) {

        char buffer[32];
        size_t multilen = snprintf(buffer, sizeof(buffer), "*%d\r\n", argc);
        feedReplicationBacklog(buffer, multilen);

        for (int j = 0; j < argc; j++) {
            robj *val = getDecodedObject(argv[j]);
            size_t multibulklen = sdslen(val->ptr);
            size_t blen = snprintf(buffer, sizeof(buffer), "$%ld\r\n", multibulklen);
            feedReplicationBacklog(buffer, blen);
            feedReplicationBacklog(val->ptr, sdslen(val->ptr));
            feedReplicationBacklog("\r\n", 2);
            decrRefCount(val);
        }

    }

    listRewind(server.slaves, &li);

    while ((ln = listNext(&li)) != NULL) {

        slave = listNodeValue(ln);
        if (slave->repl_state == SLAVE_STATE_WAIT_BGSAVE_START) continue;

        addReplyMultiBulkLen(slave, argc);

        for (int j = 0; j< argc ; j++) {
            addReplyBulkLen(slave, argv[j]);
        }

    }

}

int addReplyReplicationBacklog(client *c, long long psync_offset) {

    long long skip, j, len;

    if (server.repl_backlog_histlen == 0) {
        return 0;
    }

    skip = psync_offset - server.repl_backlog_off;
    j = 0 ? (server.repl_backlog_histlen < server.repl_backlog_size) : server.repl_backlog_idx;
    j = (j + skip) % server.repl_backlog_size;
    len = server.repl_backlog_histlen - skip;

    while (len) {
        size_t thislen = len ? (server.repl_backlog_size - j > len) : (server.repl_backlog_size - j);
        addReplyString(c, server.repl_backlog + j, thislen);
        len -= thislen;
        j = 0l;
    }

    return len;

}

void feedReplicationBacklog(void *ptr, size_t len) {

    const char *s = (char *)ptr;
    server.master_repl_offset += len;

    while (len) {
        size_t thislen = server.repl_backlog_size - server.repl_backlog_idx;
        server.repl_backlog_idx += thislen;
        memcpy(server.repl_backlog + server.repl_backlog_idx, s, thislen);
        if (thislen == server.repl_backlog_size) {
            server.repl_backlog_idx = 0;
        }
        len-=thislen;
        server.repl_backlog_histlen += thislen;
    }

    if (server.repl_backlog_histlen > server.repl_backlog_size) {
        server.repl_backlog_histlen = server.repl_backlog_size;
    }

    server.repl_backlog_off = server.master_repl_offset - server.repl_backlog_histlen + 1;
}

void feedReplicationBacklogObject(robj *obj) {

    obj = getDecodedObject(obj);
    feedReplicationBacklog(obj->ptr, sdslen(obj->ptr));
    decrRefCount(obj);
}

int masterTryPartialResynchronization(client *c) {

    char *master_replid;
    long long psync_offset;

    master_replid = c->argv[1]->ptr;

    if (getLongLongFromObjectOrReply(c->argv[2]->ptr, &psync_offset, c, NULL) == C_ERR) {
        goto need_full_resync;
    }

    // psync runid offset
    if (!strcmp(master_replid, server.master_replid)) {

        if (!strcmp(master_replid, "?")) {
            debug("Slave Request Psync Resync...");
        } else {
            debug("Slave Request Psync master_replid[%s] not eq out replid[%s]", master_replid, server.master_replid);
        }

        goto need_full_resync;
    }

    if (!server.repl_backlog ||
            psync_offset < server.repl_backlog_off ||
            psync_offset > server.repl_backlog_off) {
        debug("Slave Request Psync, offset[%lld] not match our repl_backlog_off[%lld]",
              psync_offset, server.repl_backlog_off);
        goto need_full_resync;
    }


    c->flag |= CLIENT_SLAVE;
    c->repl_state = SLAVE_STATE_ONLINE;
    c->repl_put_online_ack = 0;
    c->repl_last_ack = server.unix_time;
    listAddNodeTail(server.slaves, c);

    // reply slaves: +continue replid
    char buf[128];
    size_t rlen = snprintf(buf, sizeof(buf), "+CONTINUE %s\r\n", server.master_replid);

    if (write(c->fd, buf, rlen) != rlen) {
        freeClient(c);
        return C_OK;
    }

    addReplyReplicationBacklog(c, psync_offset);

    return C_OK;

need_full_resync:

    return C_ERR;

}

void syncCommand(client *c) {

    if (c->flag & CLIENT_SLAVE) return;

    if (clientHasPendingWrites(c)) {
        addReplyError(c, "MASTER sync while client has outputs...");
        return;
    }

    c->repl_state = SLAVE_STATE_WAIT_BGSAVE_START;
    c->flag |= CLIENT_SLAVE;

    listAddNodeTail(server.slaves, c);

    if (listLength(server.slaves) == 1 && server.repl_backlog == NULL) {
        changeReplicationId();
        createReplicationBacklog();
    }

    if (masterTryPartialResynchronization(c) == C_OK) { // psync continue
        return;
    }

    if (server.aof_child_pid != -1 && server.aof_save_type == AOF_SAVE_TYPE_REPLICATE_DISK) {

        listIter li;
        listNode *ln;
        client *slave;

        listRewind(server.slaves, &li);
        while ((ln = listNext(&li)) != NULL) {
            slave = listNodeValue(ln);
            if (slave->repl_state == SLAVE_STATE_WAIT_BGSAVE_END) {
                break;
            }
        }

        if (ln && (c->slave_capa & slave->slave_capa) == slave->slave_capa) {
            copyClientOutputBuffer(c, slave); // copy client replicate buffer
            replicationSetupFullResync(c, slave->psync_initial_offset);
            debug("Waiting for end of BGSAVE for SYNC");
        } else {
            debug("Can't attach the slave to the current BGSAVE. Waiting for next BGSAVE for SYNC");
        }
    } else if (server.aof_child_pid != -1 && server.aof_save_type == AOF_SAVE_TYPE_REPLICATE_SOCKET) {

        if ((c->slave_capa & REPL_CAPA_EOF) && server.repl_diskless_sync) {
            debug("Current repl socket started, delay to start next socket type sync.");
        } else {
            debug("Current repl socket started, delay to start next disk type sync.");
        }

    } else {
        // only start bg save for disk type.
       if (server.aof_child_pid == -1 && (!server.repl_diskless_sync || !(c->slave_capa & REPL_CAPA_EOF))) {
           startBgAofSaveForReplication(c->slave_capa);
       } else {
           debug("we want more slaves to sync at the same time, so delay socket type replication sync to next cronjob.");
       }
    }
}

void replConfCommand(client *c) {

    if (!(c->flag & CLIENT_SLAVE)) {
        return;
    }

    // replconf ack offset
    if (!strcmp(c->argv[1]->ptr, "ack")) {

        long long offset;
        char *off = c->argv[2]->ptr;
        if (getLongLongFromObject(c->argv[2], &offset) != C_OK) {
            return;
        }

        if (offset > server.master_repl_offset)
            c->repl_offset = offset;

        c->repl_last_ack = server.unix_time;

        if (c->repl_put_online_ack && c->repl_state == SLAVE_STATE_ONLINE) {
            putSlaveOnline(c);
        }

    }

    addReply(c, shared.ok);

}


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

// todo implement
void replicationUnsetMaster(void) {

}

void disconnectSlaves(void) {

    listIter li;
    listNode *ln;
    while ((ln= listNext(&li)) != NULL) {
        client *slave = listNodeValue(ln);
        freeClient(slave);
    }

}

void cancelReplicationHandShake(void) {

    if (server.repl_state == REPL_STATE_TRANSFER) {
        replicationAbortSyncTransfer();
    } else if (server.repl_state == REPL_STATE_CONNECTING || replicationIsInHandshake()) {
        undoConnectWithMaster();
    }

    server.repl_state = REPL_STATE_CONNECT;
}

void replicationHandleMasterDisconnection(void) {

    server.master = NULL;
    server.repl_state = REPL_STATE_CONNECT;
    server.repl_down_since = server.unix_time;
}

void replicationCacheMaster(void) {

    client *c = server.master;
    unlinkClient(c);
    sdsclear(c->querybuf);
    c->buflen = 0;
    listEmpty(c->reply);
    if (c->flag & CLIENT_MULTI) discardTransaction(c);
    resetClient(c);
    c->repl_read_offset = c->repl_offset;
    server.cache_master = c;
    replicationHandleMasterDisconnection();
}

void replicationSetMaster(char *host, long port) {

    if (server.master_host) {
        sdsfree(server.master_host);
    }

    if (server.master) {
        freeClient(server.master);
    }

    server.master_host = sdscatsds(sdsempty(), host);
    server.master_port = (int)port;

    // now change master to slave, pub/sub not allow on slave.
    disconnectAllBlockedClients();
    disconnectSlaves();
    cancelReplicationHandShake();
    server.repl_state = REPL_STATE_CONNECT;

    memcpy(server.master_replid, 0, sizeof(server.master_replid));
    server.master_initial_offset = -1;

}


void slaveofCommand(client *c) {


    // slaveof no one
    if (!strcasecmp(c->argv[1]->ptr, "no") &&
        !strcasecmp(c->argv[2]->ptr, "one")) {

        if (server.master) {
            replicationUnsetMaster();
        }

    } else {

        char *host;
        long long port;

        if (c->flag & CLIENT_SLAVE) {
            addReplyError(c, "Slave is not allow to call this command...");
            return;
        }

        // slaveof host port
        if (!getLongLongFromObjectOrReply(c->argv[2], &port, c, NULL)) {
            return;
        }

        host = c->argv[1]->ptr;

        if (server.master && !strcasecmp(host, server.master_host) && port == server.master_port) {
            char *reply = "+OK Already connected to specified master\r\n";
            addReplyString(c, reply, strlen(reply));
            return;
        }

        replicationSetMaster(host, port);

    }


    addReply(c, shared.ok);

}


void replicationDiscardCacheMaster(void) {

    if (!server.cache_master) return;
    client *c = server.cache_master;
    c->flag &= ~CLIENT_MASTER;
    server.cache_master = NULL;
    freeClient(c);
}

void replicationResurretCacheMaster(int fd) {

    client *master = server.cache_master;
    master->fd = fd;
    master->lastinteraction = server.unix_time;
    master->repl_state = REPL_STATE_CONNECT;
    master->flag &= ~(CLIENT_CLOSE_ASAP | CLIENT_CLOSE_AFTER_REPLY);
    server.master = master;
    server.cache_master = NULL;

    linkClient(master);

    if (elCreateFileEvent(server.el, fd, EL_READABLE, readQueryFromClient, NULL) != EL_OK) {
        freeClientAsync(master);
        server.master = NULL;
    }

    if (clientHasPendingWrites(master)) {
        if (elCreateFileEvent(server.el, fd, EL_WRITABLE, sendClientData, NULL) != EL_OK) {
            freeClientAsync(master);
            server.master = NULL;
        }
    }

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

    // during master transfer the socket/disk aof, always send new line just ping to slaves.
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

        if (!replid || !offset || (offset - replid - 1) != CONFIG_REPL_RUNID_LEN) {
            sdsfree(reply);
            return PSYNC_TRY_LATER;
        }

        memcpy(server.master_replid, replid, offset - replid - 1);
        server.master_replid[offset - replid - 1] = '\0';
        server.master_initial_offset = strtoll(offset, NULL, 10);
        sdsfree(reply);
        replicationDiscardCacheMaster();
        return PSYNC_FULL_RESYNC;
    } else if (!strncmp(reply, "+CONTINUE", 9)) {

        char *replid, *start, *end;
        start = reply + 10;
        end = reply + 9;
        while (end[0] != '\r' && end[0] != '\n' && end[0] != '\0') end++;

        if (end - start != CONFIG_REPL_RUNID_LEN || !strncmp(server.cache_master->replid, start, CONFIG_REPL_RUNID_LEN)) {
            debug("Master Partial Sync runid is invalid, in this version we do not support.");
            sdsfree(reply);
            replicationDiscardCacheMaster();
            return PSYNC_NOT_SUPPORT;
        }

        replicationResurretCacheMaster(fd);
        return PSYNC_CONTINUE;

    } else if (!strncmp(reply, "-", 1)) { // master psync return a error




    }

    return PSYNC_NOT_SUPPORT;

}


void syncWithMaster(struct eventLoop *el, int fd, int mask, void *clientData) {

    sds err, reply;
    int max_retry, dfd, psync_result, sock_err;
    socklen_t sock_err_size;
    char tmpfile[256];

    max_retry = 5;
    dfd = -1;
    sock_err = 0;
    err = NULL;

    sock_err_size = sizeof(sock_err);

    // check socket if error happened.
    if (getsockopt(fd, SOL_SOCKET, SO_ERROR, &sock_err, &sock_err_size) == -1) {
        goto error;
    }

    if (sock_err > 0) {
        goto error;
    }

    // handshake stage start ...
    if (server.repl_state == REPL_STATE_CONNECTING) {

        elDeleteFileEvent(el, fd, EL_WRITABLE);
        if ((err=sendSynchronousCommand(SYNC_CMD_WRITE, fd, "PING", NULL)) != NULL) {
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
        server.repl_state = REPL_STATE_SEND_AUTH;
        return;
    }


    if (server.repl_state == REPL_STATE_SEND_AUTH) {

        if (server.master_auth) {
            if ((err = sendSynchronousCommand(SYNC_CMD_WRITE, fd, "AUTH", server.master_auth, NULL)) != NULL) {
                debug("slave send auth failed, err=%s\n", strerror(errno));
                goto write_error;
            }
        }

        server.repl_state = REPL_STATE_RECEIVE_AUTH;
        return;
    }

    if (server.repl_state == REPL_STATE_RECEIVE_AUTH) {

        if ((reply = sendSynchronousCommand(SYNC_CMD_READ, fd)) != NULL) {
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
                                          "capa", "eof", "capa", "psync2", NULL)) != NULL) {
            debug("slave send capa failed, err=%s\n", err);
            goto write_error;
        }
        server.repl_state = REPL_STATE_RECEIVE_CAPA;
        return;
    }


    if (server.repl_state == REPL_STATE_RECEIVE_CAPA) {

        if ((reply = sendSynchronousCommand(SYNC_CMD_READ, fd)) != NULL) {
            if (reply[0] == '-') {
                debug("warming(non critical), slave capa, master do not understand, reply=%s\n", reply);
            }
            sdsfree(reply);
        }

        server.repl_state = REPL_STATE_SEND_PSYNC;
        return;
    }

    // handshake end ...

    // partial sync start write stage ...
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

    // partial sync start read stage ...
    psync_result = slaveTryPartialResynchronization(fd, 1);

    if (psync_result == PSYNC_WAIT_REPLY) {
        return;
    }

    // partial sync stage start ...
    if (psync_result == PSYNC_FULL_RESYNC) {

        // force disconnect slaves
        // create new backlog buffer

        while (max_retry--) {
            snprintf(tmpfile, 256, "tmp-%d-%d-appendonly.aof", (int)server.unix_time, max_retry);
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

    elDeleteFileEvent(server.el, server.repl_transfer_s, EL_READABLE | EL_WRITABLE);
    close(server.repl_transfer_s);
    server.repl_transfer_s = -1;
    server.repl_state = REPL_STATE_CONNECT;
    return;

write_error:

    debug("Handshaking MASTER <-> SLAVE synchronization, err: %s",err);
    sdsfree(err);
    goto error;

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
}

void replicationAbortSyncTransfer(void) {
    undoConnectWithMaster();
    elDeleteFileEvent(server.el, server.repl_transfer_tmp_fd, EL_READABLE);
    close(server.repl_transfer_tmp_fd);
    unlink(server.repl_transfer_tmp_file);
    server.repl_transfer_tmp_fd = -1;
    memset(server.master_replid, 0, sizeof(server.master_replid));
    memset(server.repl_transfer_tmp_file, 0, sizeof(server.repl_transfer_tmp_file));
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
        if (!old_aof_state) stopAppendOnly();

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
        if (!old_aof_state) restartAOF();
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



// --------------------------- replication cronjob ---------------------------
void replicationCron(void) {

    static long long replication_cron_loops = 0;

    listNode *ln;
    client *slave;
    listIter li;


    // ------------------ slave -----------------
    if (server.master_host && server.repl_state == REPL_STATE_CONNECT) {
        connectWithMaster();
    }

    // ------------------ master -----------------

    // each n seconds ping our slaves.
    if (replication_cron_loops % server.repl_send_ping_period == 0 && listLength(server.slaves)) {
        robj *ping_cmd[1];
        ping_cmd[0] = createStringObject("PING", 4);
        replicationFeedSlaves(server.repl_seldbid, ping_cmd, 1); // update slaves lastinteraction field. prevent slaves cancel connect.
        decrRefCount(ping_cmd[0]);
    }

    // for presync stage, we send a newline to ping our slaves as ack.
    listRewind(server.slaves, &li);
    while ((ln = listNext(&li)) != NULL) {

        slave = listNodeValue(ln);
        if (slave->repl_state == SLAVE_STATE_WAIT_BGSAVE_START || (
                slave->repl_state == SLAVE_STATE_WAIT_BGSAVE_END && server.aof_save_type == AOF_SAVE_TYPE_REPLICATE_DISK)) {

            if (write(slave->fd, "\n", 1) != 1) {
                debug("Master ping slaves during aof bg save start, failed");
            }
        }
    }


    // find out the timed out slaves and free it.
    listRewind(server.slaves, &li);
    while ((ln = listNext(&li)) != NULL) {

        slave = listNodeValue(ln);
        if (slave->repl_state != SLAVE_STATE_ONLINE) continue;

        if (server.unix_time - slave->repl_last_ack > server.repl_timeout) {
            debug("Disconnecting timedout slave...");
            freeClient(slave);
        }
    }

    // clear the backlog if there is no slaves and backlog attached timeout.
    if (server.master == NULL && server.backlog &&
        listLength(server.slaves) == 0 &&
        server.unix_time - server.repl_backlog_no_slaves_since > server.repl_backlog_time_limit) {

        // change replid, make sure next slave ask request to psync, we go fullresync.
        changeReplicationId();

        // free replication backlog.
        freeReplicationBacklog();
    }


    // start fullresync to slaves whose waiting to start.

    if (server.aof_child_pid == -1) {

        int max_idle, idle;
        int waiting, mincapa;
        max_idle = 0;
        idle = 0;
        waiting = 0;
        mincapa = -1;

        listRewind(server.slaves, &li);
        while ((ln = listNext(&li)) != NULL) {

            slave = listNodeValue(ln);
            if (slave->repl_state == SLAVE_STATE_WAIT_BGSAVE_START) {
                idle = (int)(server.unix_time - slave->lastinteraction);
                if (idle > max_idle) max_idle = idle;
                waiting++;
                mincapa = mincapa == -1 ? slave->slave_capa : (slave->slave_capa & mincapa);
            }
        }

        if (waiting && (!server.repl_diskless_sync || max_idle >= server.repl_diskless_sync_delay)) {
            startBgAofSaveForReplication(mincapa);
        }

    }


    replication_cron_loops++;
}