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


void syncWithMaster(struct eventLoop *el, int fd, int mask, void *clientData) {

    sds err, reply;


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
                sdsfree(reply);
                goto error;
            }
            sdsfree(reply);
        }

        server.repl_state = REPL_STATE_SEND_PSYNC;
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