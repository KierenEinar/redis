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


int main(int argc, char **argv) {
    printf("server start....\r\n");
    server.el = elCreateEventLoop(1024);
    elMain(server.el);
    return 0;
}
