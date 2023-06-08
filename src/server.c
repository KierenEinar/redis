//
// Created by kieren jiang on 2023/2/20.
//

#include "server.h"
#include "varint.h"

struct redisServer server;

mstime_t mstime() {
    struct timeval t;
    gettimeofday(&t, NULL);
    mstime_t milliseconds= 0;
    milliseconds = t.tv_sec * 1000 + t.tv_usec / 1000;
    return milliseconds;
}


void exitFromChild(int code) {
    _exit(code);
}

int listenPort(int backlog) {

    int unsupported = 0;

    // bind and listen inet_v6
    server.ipfd[server.ipfd_count] = anetTcp6Server(server.neterr, server.port, backlog);
    if (server.ipfd[server.ipfd_count] != ANET_ERR) {
        anetNonBlock(server.ipfd[server.ipfd_count]);
        fprintf(stdout, "server listen, fd=%d\r\n", server.ipfd[server.ipfd_count]);
        server.ipfd_count++;
    } else if (errno == EAFNOSUPPORT) {
        unsupported++;
    }

    // bind and listen inet_v4
    server.ipfd[server.ipfd_count] = anetTcpServer(server.neterr, server.port, backlog);
    if (server.ipfd[server.ipfd_count] != ANET_ERR) {
        anetNonBlock(server.ipfd[server.ipfd_count]);
        fprintf(stdout, "server listen, fd=%d\r\n", server.ipfd[server.ipfd_count]);
        server.ipfd_count++;
    } else if (errno == EAFNOSUPPORT) {
        unsupported++;
    }

    if (server.ipfd_count == 0) {
        return C_ERR; // non ip address binded
    }

    return C_OK;
}

void beforeSleep (struct eventLoop *el) {
    handleClientsPendingWrite();
}

long long serverCron(struct eventLoop *el, int id, void *clientData) {
    freeClientInFreeQueueAsync();

    return SERVER_CRON_PERIOD_MS;
}


void initServer() {
    server.backlog = DEFAULT_BACKLOG;
    server.port = DEFAULT_BIND_PORT;
    server.unix_time = time(NULL);
    server.el = elCreateEventLoop(1024);
    server.client_pending_writes = listCreate();
    server.client_list = listCreate();
    server.client_close_list = listCreate();
    listSetFreeMethod(server.client_list, zfree); // free the client which alloc from heap

    if (listenPort(server.backlog) == C_ERR) {
        exit(1);
    }

    for (int i=0; i<server.ipfd_count; i++) {
        int fd = server.ipfd[i];
        if (elCreateFileEvent(server.el, fd, EL_READABLE, acceptTcpHandler, NULL) == EL_ERR) {
            exit(1);
        }
    }

    elSetBeforeSleepProc(server.el, beforeSleep);

    elCreateTimerEvent(server.el, SERVER_CRON_PERIOD_MS, serverCron, NULL, NULL);


}




int main(int argc, char **argv) {
    printf("server start...., pid=%d\r\n", getpid());
    initServer();
    elMain(server.el);
    return 0;
}



