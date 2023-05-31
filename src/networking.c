//
// Created by kieren jiang on 2023/5/29.
//

#include "server.h"

void writeTcpHandler(eventLoop *el, int fd, int mask, void *clientData) {
    write(fd, "success\r\n", strlen("success\r\n"));
    elDeleteFileEvent(el, fd, EL_WRITABLE);
    return;
}

void readTcpHandler(eventLoop *el, int fd, int mask, void *clientData) {

    char buf[128];
    ssize_t nwritten = 0;
    while (1) {
        nwritten = read(fd, buf, sizeof(buf)-1);
        if (nwritten <= 0) return;
        printf("nwritten = %ld\r\n", nwritten);
        buf[nwritten] = '\0';
        fprintf(stdout, "get from client, content=%s\r\n", buf);

        if (!elGetFileEvent(el, fd, EL_WRITABLE))
            elCreateFileEvent(server.el, fd, EL_WRITABLE, writeTcpHandler, NULL);
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


void processInputBuffer(client *c) {

    while (c->buflen) {

        if (c->reqtype == 0) {
            if (c->querybuf[0] == '*') {
                c->reqtype = PROTO_REQ_MULTI;
            } else {
                c->reqtype = PROTO_REQ_INLINE;
            }
        }

        if (c->reqtype == PROTO_REQ_MULTI) {
            if (processMultiBulkBuffer(c) != RESP_PROCESS_OK) break;
        } else {
           if (processInlineBuffer(c) != RESP_PROCESS_OK) break;
        }

        if (c->argc == 0) {
            // todo reset client and process next command
        } else {

            // todo process command

            for (int j=0; j<c->argc; j++) {
                fprintf(stdout, "argv=%s\r\n", c->argv[j]);
            }

        }

    }


}

void readQueryFromClient(eventLoop *el, int fd, int mask, void *clientData) {

    client *c = (client*) clientData;
    size_t nread;
    size_t readlen = PROTO_NREAD;

    if (c->bufcap - c->buflen < readlen) {
        if (c->querybuf == NULL) {
            c->querybuf = malloc(readlen);
            c->bufcap += readlen;
        } else {
            // todo realloc fail, set protocol error
            if ((c->querybuf = zrealloc(c->querybuf, c->bufcap+readlen)) == NULL) {
                return;
            }
            c->bufcap += readlen;
        }
    }
    fprintf(stdout, "fd=%d, mask=%d\r\n", fd, mask);
    nread = read(fd, c->querybuf+c->buflen, readlen);
    if (nread == -1) {
        fprintf(stdout, "readQueryFromClient, err=%s\r\n", strerror(errno));
        if (errno == EAGAIN) return;
        return;
    } else if (nread == 0) {
        // client fin connect
        // todo releaseclient
        return;
    }

    c->buflen+=nread;
    c->querybuf[c->buflen] = '\0';
    processInputBuffer(c);

    return;
}


void acceptCommandHandler(int cfd, char *ip, int port) {

    client *c = zmalloc(sizeof(c));
    anetNonBlock(cfd);
    if (!elCreateFileEvent(server.el, cfd, EL_READABLE, readQueryFromClient, c)) {
        // todo free something
        return;
    }

    c->querybuf = NULL;
    c->buflen = 0;
    c->bufcap = 0;
    c->multilen = 0;
    c->bulklen = -1;
    c->argvlen = 0;
    c->argv = NULL;
    c->argc = 0;

    c->fd = cfd;
    c->reqtype = 0;
}

int processMultiBulkBuffer(client *c){
    /* process RESP mulit bulk buffer,
     e.g. *3\r\n
          $3\r\n
          set\r\n
          $1\r\n
          a\r\n
          $1\r\n
          b\r\n
    */
    fprintf(stdout, "processMultiBulkBuffer msg=%s\r\n", c->querybuf);
    long pos = 0;
    if (c->multilen == 0) {
        char *newline = strchr(c->querybuf, '\r');
        fprintf(stdout, "buflen=%ld, bufcap=%ld\r\n", c->buflen, c->bufcap);
        if (newline == NULL) {
            if (c->buflen >= RESP_PROTO_MAX_INLINE_SEG) {
                char errmsg[] = "inline len limit 64k";
                memcpy(c->err, errmsg, sizeof(errmsg));
            }
            return RESP_PROCESS_ERR;
        }

        // check if contains \n
        if (newline - c->querybuf > c->buflen -2) {
            return RESP_PROCESS_ERR;
        }

        long value;
        fprintf(stdout, "s=%s\r\n", c->querybuf+1);
        int ok = string2l(c->querybuf+1, newline-(c->querybuf+1), &value);
        if (!ok || value <=0 || value >= 1024 * 1024) {
            char errmsg[] = "multi len limit";
            memcpy(c->err, errmsg, sizeof(errmsg));
            return RESP_PROCESS_ERR;
        }

        pos+=newline-c->querybuf+2;

        c->multilen = value;

        if (c->argv) {
            for (long j =0; j<c->multilen; j++) {
                free(c->argv[j]);
            }
            free(c->argv);
        }
        c->argv = malloc(sizeof(char*) * c->multilen);
    }

    while (c->multilen) {

        // parse bulk len
        if (c->bulklen == -1) {

            // find *x\r\n
            char *newline = strchr(c->querybuf+pos,'\r');
            if (newline == NULL) {
                if (c->buflen >= RESP_PROTO_MAX_INLINE_SEG) {
                    char errmsg[] = "inline len limit 64k";
                    memcpy(c->err, errmsg, sizeof(errmsg));
                }
                break;
            }

            // must contains \n
            if (newline-(c->querybuf) > (c->buflen - 2)) {
                break;
            }

            long value;
            int ok = string2l(c->querybuf+pos+1, newline-(c->querybuf+pos+1), &value);
            if (!ok || value <= 0 || value >= RESP_PROTO_MAX_BULK_SEG) {
                char errmsg[] = "bulklen limit 512M";
                memcpy(c->err, errmsg, sizeof(errmsg));
                return RESP_PROCESS_ERR;
            }

            pos+=(newline-(c->querybuf+pos)) + 2;

            c->bulklen = value;

        }

        // not enough data to read
        if (c->bulklen > (c->buflen-pos) + 2) {
            break;
        } else {
            char *str = malloc(sizeof(char) * (c->bulklen + 1));
            memcpy(str, c->querybuf+pos, c->bulklen);
            str[c->bulklen] = '\0';
            c->argv[c->argvlen++] = str;
            pos+=c->bulklen+2;
            c->bulklen = -1;
            c->multilen--;
        }

    }

    // trim querybuf
    if (pos) {
        c->buflen = c->buflen - pos;
        memmove(c->querybuf, c->querybuf+pos, c->buflen);
        if (c->buflen>0) c->querybuf[c->buflen] = '\0';
    }

    if (c->multilen == 0) {
        c->reqtype = 0;
        fprintf(stdout, "processMultiBulkBuffer success\r\n");
        return RESP_PROCESS_OK;
    }

    return RESP_PROCESS_ERR;
}

int processInlineBuffer(client *client) {

    fprintf(stdout, "receive msg=%s\r\n", client->querybuf);
    memset(client->querybuf, 0, client->buflen);
    client->buflen = 0;
    client->reqtype = 0;
    return RESP_PROCESS_OK;
}