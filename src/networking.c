//
// Created by kieren jiang on 2023/5/29.
//

#include "server.h"

void readTcpHandler(eventLoop *el, int fd, int mask, void *clientData) {

    char buf[128];
    ssize_t nwritten = 0;
    while (1) {
        nwritten = read(fd, buf, sizeof(buf)-1);
        if (nwritten <= 0) return;
        printf("nwritten = %ld\r\n", nwritten);
        buf[nwritten] = '\0';
        fprintf(stdout, "get from client, content=%s\r\n", buf);
        write(fd, "success\r\n", strlen("success\r\n"));
    }

}


void acceptTcpHandler(eventLoop *el, int fd, int mask, void *clientData) {

    char cip[129];
    int cfd, cport, max = 1000;

    while (max--) {
        cfd = anetTcpAccept(server.neterr, fd, cip, sizeof(cip), &cport);
        if (cfd == ANET_ERR) return;
        acceptCommandHandler(cfd, cip, cport);
    }
}

void acceptCommandHandler(int cfd, char *ip, int port) {
    anetNonBlock(cfd, 0);
    elCreateFileEvent(server.el, cfd, EL_READABLE, readTcpHandler,NULL);

}



