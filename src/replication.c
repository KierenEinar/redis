//
// Created by kieren jiang on 2023/9/25.
//
#include "server.h"

// ------------------------- slave ----------------------

#define SYNC_CMD_READ (1<<0)
#define SYNC_CMD_WRITE (1<<1)
sds sendSynchronousCommand(int flags, int fd, ...) {

    if (flags & SYNC_CMD_WRITE) {


    }


    return NULL;
}


void syncWithMaster(struct eventLoop *el, int fd, int mask, void *clientData) {


    // todo check the socket


    if (server.repl_state == REPL_STATE_CONNECTING) {

        elDeleteFileEvent(el, fd, EL_WRITABLE);
        if (sendSynchronousCommand(SYNC_CMD_WRITE, fd, "PING", NULL) != NULL) {
            goto write_error;
        }
        server.repl_state = REPL_STATE_RECEIVE_PONG;
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