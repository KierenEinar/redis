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


int _anetTcpGenericConnect(char *host, int port, char *sourceaddr, int flags) {

    char _port[6], err[255];
    struct addrinfo hints, *serverinfo, *bserverinfo, *p, *bp;
    int error, s;

    error = 0;
    snprintf(_port, sizeof(_port), "%d", port);
    memset(&hints, 0, sizeof(hints));

    hints.ai_family = AF_UNSPEC;
    hints.ai_socktype = SOCK_STREAM;

    if ((error = getaddrinfo(host, _port, &hints, &serverinfo)) == -1) {
        debug("anetGenericConnect getaddrinfo, err=%s\n", gai_strerror(error));
        return ANET_ERR;
    }

    for (p=serverinfo; p; p=serverinfo->ai_next) {
        if ((s = socket(p->ai_family, p->ai_socktype, p->ai_protocol)) == -1)
            continue;

        if (anetReuseAddr(err, s) == ANET_ERR) {
            debug("anetTcpGenericConnect anetReuseAddr failed\n");
            goto error;
        }

        if (flags & O_NONBLOCK && anetNonBlock(s) == ANET_ERR) {
            debug("anetTcpGenericConnect anetNonBlock socket failed\n");
            goto error;
        }

        if (sourceaddr) {

            if ((error = getaddrinfo(sourceaddr, NULL, &hints, &bserverinfo)) == -1) {
                debug("anetGenericConnect getaddrinfo, sourceaddr=%s, err=%s\n", sourceaddr, gai_strerror(error));
                goto  error;
            }

            for (bp = bserverinfo; bp; bp = bserverinfo->ai_next) {

                if (bind(s, bp->ai_addr, bp->ai_addrlen) == -1) {
                    debug("warnimg anetTcpGenericConnect bind sourceaddr failed, err=%s\n", strerror(error));
                    error = errno;
                    continue;
                }

                error = 0;
                break;
            }

            freeaddrinfo(bserverinfo);
            if (error != 0) goto error;

        }

        if (connect(s, p->ai_addr, p->ai_addrlen) == -1) {
            if (errno == EINPROGRESS && flags & O_NONBLOCK) {
                goto end;
            }

            close(s);
            s = ANET_ERR;
            continue;
        }

        goto end;
    }

error:
    if (s != ANET_ERR) close(s);
    s = ANET_ERR;

end:
    freeaddrinfo(serverinfo);
    if (sourceaddr && error != 0)
        return _anetTcpGenericConnect(host, port, NULL, flags);
    return s;
}

int anetSetBlock(int fd, int block) {
    int flag = fcntl(fd, F_GETFL);
    if (block) {
        flag &= ~O_NONBLOCK;
    } else {
        flag |= O_NONBLOCK;
    }

    if (fcntl(fd, F_SETFL, flag) == -1)
        return ANET_ERR;

    return ANET_OK;
}

int anetNonBlock(int fd) {
    return anetSetBlock(fd, 0);
}

int anetBlock(int fd) {
    return anetSetBlock(fd, 1);
}

int anetTcpNoDelay(int fd) {
    socklen_t onlen;
    int on, retval;
    on = 1;
    onlen = sizeof(onlen);
    if ((retval = setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &on, onlen)) == -1) {
        debug("server setsockopt tcp no_delay failed, err=%s", strerror(errno));
        return C_ERR;
    }
    return C_OK;
}

int anetTcpSendTimeout(int fd, long timeout) {

    struct timeval tv;
    socklen_t tvlen;
    int retval;
    tv.tv_sec = timeout / 1000;
    tv.tv_usec = (int)(timeout % 1000);
    tvlen = sizeof(tv);
    if ((retval = setsockopt(fd, IPPROTO_TCP, SO_SNDTIMEO, &tv, tvlen)) == -1) {
        return C_ERR;
    }

    return C_OK;
}

int anetTcpRecvTimeout(int fd, long timeout) {

    struct timeval tv;
    socklen_t tvlen;
    int retval;
    tv.tv_sec = timeout / 1000;
    tv.tv_usec = (int)(timeout % 1000);
    tvlen = sizeof(tv);
    if ((retval = setsockopt(fd, IPPROTO_TCP, SO_RCVTIMEO, &tv, tvlen)) == -1) {
        return C_ERR;
    }

    return C_OK;

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

    fprintf(stdout, "anetTcpAccept cfd=%d\n", cfd);

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

    fprintf(stdout, "accept, ip=%s, port=%d\n", ip, *port);
    return cfd;
}

int anetTcpConnect(char *host, int port, char *sourceaddr, int flags) {
    return _anetTcpGenericConnect(host, port, sourceaddr, flags);
}

void closeListeningSockets() {
    int j;

    for (j=0; j<server.ipfd_count; j++) {
        elDeleteFileEvent(server.el, server.ipfd[j], EL_READABLE|EL_WRITABLE);
        close(server.ipfd[j]);
    }
}
