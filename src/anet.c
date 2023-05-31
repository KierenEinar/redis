//
// Created by kieren jiang on 2023/5/29.
//

#include "anet.h"

void anetSetError(char *err, char *fmt, ...) {

    if (!err) return;
    va_list ap;
    va_start(ap, fmt);
    vsnprintf(err, SERVER_NETWORK_ERR_LEN, fmt, ap);
    va_end(ap);
}

int anetReuseAddr(char *err, int socketfd) {

    int reuse = 1;

    if (setsockopt(socketfd, SOL_SOCKET, SO_REUSEADDR,&reuse, sizeof(reuse)) != 0) {
        anetSetError(err, "anetReuseAddr err=%s", strerror(errno));
        return ANET_ERR;
    }
    return ANET_OK;
}

int anetListen(char *err, int socketfd, struct sockaddr* addr, socklen_t slen, int backlog) {

    if (bind(socketfd, addr, slen) == -1) {
        anetSetError(err, "anetListen bind err=%s", strerror(errno));
        return ANET_ERR;
    }

    if (listen(socketfd, backlog) == -1) {
        anetSetError(err, "anetListen listen err=%s", strerror(errno));
        return ANET_ERR;
    }

    return ANET_OK;
}


static int _anetTcpServer(char *err, int port, int af_family, int backlog) {

    int rv, s=-1;

    struct addrinfo hint, *serveInfo, *p;
    char _port[6]; // 65535
    snprintf(_port, sizeof(_port), "%d", port);

    memset(&hint, 0, sizeof(hint));
    hint.ai_socktype = SOCK_STREAM;
    hint.ai_family = af_family;
    hint.ai_flags = AI_PASSIVE;

    if ( (rv = getaddrinfo(NULL, _port, &hint, &serveInfo) ) !=0 ) {
        anetSetError(err, "getaddrinfo err=%s", gai_strerror(rv));
        return ANET_ERR;
    }

    for ( p = serveInfo; p != NULL; p = p->ai_next) {
        if ((s = socket(p->ai_family, p->ai_socktype, p->ai_protocol)) == -1) {
            continue;
        }

        if (anetReuseAddr(err, s) == ANET_ERR) goto error;
        if (anetListen(err, s, (struct sockaddr*)p->ai_addr, p->ai_addrlen, backlog) == ANET_ERR) goto error;
        goto end;
    }

    if (p == NULL) goto error;

error:
    if (s != -1) close(s);
    return ANET_ERR;
end:
    return s;
}

void anetSetBlock(int fd, int block) {
    int flag = fcntl(fd, F_GETFL);
    if (block) {
        flag &= ~O_NONBLOCK;
    } else {
        flag |= O_NONBLOCK;
    }

    fcntl(fd, F_SETFL, flag);
}

void anetNonBlock(int fd) {
    anetSetBlock(fd, 0);
}

void anetBlock(int fd) {
    anetSetBlock(fd, 1);
}

int anetTcpServer(char *err, int port, int backlog) {
    return _anetTcpServer(err, port, AF_INET, backlog);
}

int anetTcp6Server(char *err, int port, int backlog) {
    return _anetTcpServer(err, port, AF_INET6, backlog);
}

int anetTcpAccept(char *err, int fd, char *ip, size_t iplen, int *port) {

    struct sockaddr_storage addr;
    socklen_t slen = sizeof(addr);
    int cfd;

    while (1) {
        cfd = accept(fd, (struct sockaddr*)&addr, &slen);
        if (cfd == -1) {
            if (errno == EINTR) continue;
            anetSetError(err, "anet tcp accept err=%s", strerror(errno));
            return ANET_ERR;
        }
        break;
    }

    fprintf(stdout, "anetTcpAccept cfd=%d\r\n", cfd);

    if (addr.ss_family == AF_INET) {
        struct sockaddr_in *in = (struct sockaddr_in *)&addr;
        if (ip) inet_ntop(AF_INET, &in->sin_addr, ip, iplen);
        if (port) *port = ntohs(in->sin_port);
    }

    if (addr.ss_family == AF_INET6) {
        struct sockaddr_in6 *in6 = (struct sockaddr_in6 *)&addr;
        if (ip) inet_ntop(AF_INET6, &in6->sin6_addr, ip, iplen);
        if (port) *port = ntohs(in6->sin6_port);
    }

    fprintf(stdout, "accept, ip=%s, port=%d\r\n", ip, *port);
    return cfd;
}

