//
// Created by kieren jiang on 2023/2/20.
//

#include "server.h"
#include "varint.h"


// global vars
struct redisServer server;
struct redisSharedObject shared;

struct redisCommand redisCommandTable[] = {
    {"get", getCommand, 2},
};

mstime_t mstime() {
    struct timeval t;
    gettimeofday(&t, NULL);
    mstime_t milliseconds= 0;
    milliseconds = t.tv_sec * 1000 + t.tv_usec / 1000;
    return milliseconds;
}


void createSharedObject(void) {

    shared.crlf = createStringObject("\r\n", 2);
    shared.ok = createStringObject("+ok\r\n", 5);
    shared.syntaxerr = createStringObject("-ERR syntax err\r\n", 17);
    shared.nullbulk = createStringObject("$-1\r\n", 5);
    for (long j=0; j<OBJ_SHARED_INTEGERS; j++) {
        shared.integers[j] = createObject(REDIS_OBJECT_STRING, (void*)(j));
        shared.integers[j]->encoding = REDIS_ENCODING_INT;
        makeObjectShared(shared.integers[j]);
    }

    makeObjectShared(shared.crlf);
    makeObjectShared(shared.ok);
    makeObjectShared(shared.syntaxerr);
    makeObjectShared(shared.nullbulk);
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

    createSharedObject();
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

int processCommand(client *c) {
    return C_OK;
}


int main(int argc, char **argv) {
    printf("server start...., pid=%d\r\n", getpid());
    // printf("crc32 = %x\r\n", crc32("hello world 12333", strlen("hello world 12333")));

    initServer();
    elMain(server.el);
    return 0;
}



