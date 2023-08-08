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

int prepareClientToWrite(client *c) {
    if (!(c->flag & CLIENT_PENDING_WRITE)) {
        c->flag |= CLIENT_PENDING_WRITE;
        listAddNodeTail(server.client_pending_writes, c);
    }
    return C_OK;
}

int _addReplyStringToBuffer(client *c, const char *str, size_t len) {

    size_t avaliable = PROTO_REPLY_CHUNK_BYTES - c->bufpos;

    if (listLength(c->reply) > 0)
        return C_ERR;

    if (len > avaliable)
        return C_ERR;

    memcpy(c->buf + c->bufpos, str, len);
    c->bufpos+=len;

    return C_OK;
}

int _addReplyStringToList (client *c, const char *str, size_t len) {

    //todo use sds to throttle up on get the char length

    if (listLength(c->reply) == 0) {
        char *copy = zmalloc(len+1);
        memcpy(copy, str, len);
        copy[len] = '\0';
        listAddNodeHead(c->reply, copy);
    } else {

        listNode *current = listLast(c->reply);
        char *cstr = (char *)current->value;
        size_t slen = strlen(str);
        if (slen + len < PROTO_REPLY_CHUNK_BYTES) {
            cstr = realloc(cstr, slen + len);
            memcpy(cstr+slen, str, len);
            current->value = cstr;
        } else {
            char *copy = zmalloc(len+1);
            memcpy(copy, str, len);
            copy[len] = '\0';
            listAddNodeTail(c->reply, copy);
        }
    }
    return C_OK;
}

void addReply(client *c, robj *r) {

    if (sdsEncodedObject(r)) {
        addReplyString(c, r->ptr, sdslen(r->ptr));
    } else if (r->encoding == REDIS_ENCODING_INT) {
        r = getDecodedObject(r);
        addReplyString(c, r->ptr, sdslen(r->ptr));
        decrRefCount(r);
    } else {
        // todo server panic
    }

}

void addReplyLongLongPrefix(client *c, long long value, char prefix) {

    if (prefix == '$' && value < OBJ_BULK_LEN_SIZE && value >= 0) {
        addReply(c, shared.bulkhdr[value]);
        return;
    } else if (prefix == '*' && value < OBJ_BULK_LEN_SIZE && value >= 0) {
        addReply(c, shared.mbulkhdr[value]);
        return;
    }

    char buf[128];
    buf[0] = prefix;
    size_t slen = ll2string(buf+1, value);
    memcpy(buf+1+slen, shared.crlf->ptr, sdslen(shared.crlf->ptr));
    addReplyString(c, buf, slen+3);
}

void addReplyBulkLen(client *c, robj *r) {

    size_t len = 0;

    if (sdsEncodedObject(r)) {
        len = sdslen(r->ptr);
    } else if (r->encoding == REDIS_ENCODING_INT) {

        len = 1;
        long value = (long)r->ptr;
        if (value < 0) {
            len++;
            value = -value;
        }
        while ((value = value / 10) > 0) {
            len++;
        }
    } else {
        // todo server panic
    }

    if (len < OBJ_BULK_LEN_SIZE) {
        addReply(c, shared.bulkhdr[len]);
    } else {
        addReplyLongLongPrefix(c, (long)len, '$');
    }

}

void addReplyLongLong(client *c, long long value) {

    if (value == 0) {
        addReply(c, shared.czero);
    } else if (value == 1) {
        addReply(c, shared.cone);
    } else {
        addReplyLongLongPrefix(c, value, ':');
    }
}

void addReplyBulk(client *c, robj *r) {
    addReplyBulkLen(c, r);
    addReply(c, r);
    addReply(c, shared.crlf);
}

void addReplyString(client *c, const char *str, size_t len) {

    if (prepareClientToWrite(c) == C_OK)
        if (_addReplyStringToBuffer(c, str, len) != C_OK)
            _addReplyStringToList(c, str, len);
}


void addReplyError(client *c, const char *str) {
    addReplyErrorLength(c, str, strlen(str));
}

void addReplyErrorLength(client *c, const char *str, size_t len) {
    addReplyString(c, "-ERR ", 5);
    addReplyString(c, str, len);
    addReplyString(c, "\r\n", 2);
}

void addReplyMultiBulkLen(client *c, long long len) {
    if (len < OBJ_BULK_LEN_SIZE) {
        addReply(c, shared.mbulkhdr[len]);
    } else {
        addReplyLongLongPrefix(c, len, '*');
    }
}

void setProtocolError(client *c) {
    c->flag |= CLIENT_CLOSE_AFTER_REPLY;
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
            resetClient(c);
        } else {

            if (processCommand(c) == C_OK) {
                resetClient(c);
            }

//            for (int j=0; j<c->argc; j++) {
//                fprintf(stdout, "argv=%s\r\n", c->argv[j]);
//            }
//
//            c->flag |= CLIENT_CLOSE_AFTER_REPLY;
//
//            addReplyString(c, "+OK\r\n", 5);

        }
    }


}

void readQueryFromClient(eventLoop *el, int fd, int mask, void *clientData) {

    client *c = (client*) clientData;
    size_t nread;
    size_t readlen = PROTO_NREAD;

    if (c->flag & CLIENT_CLOSE_AFTER_REPLY | c->flag & CLIENT_CLOSE_ASAP) return;

    if (c->bufcap - c->buflen < readlen) {
        if (c->querybuf == NULL) {
            c->querybuf = zmalloc(readlen);
        } else {
            // todo realloc fail, set protocol error
            if ((c->querybuf = zrealloc(c->querybuf, c->bufcap+readlen)) == NULL) {
                return;
            }
        }
        c->bufcap += readlen;
    }
    fprintf(stdout, "fd=%d, mask=%d\r\n", fd, mask);
    nread = read(fd, c->querybuf+c->buflen, readlen);
    if (nread <= 0) {
        if (nread == -1) {
            if (errno == EAGAIN) return;
            freeClient(c);
        }
        return;
    }

    c->buflen+=nread;
    c->querybuf[c->buflen] = '\0';
    processInputBuffer(c);

}


void acceptCommandHandler(int cfd, char *ip, int port) {

    client *c;
    c = zmalloc(sizeof(*c));
    anetNonBlock(cfd);
    if (EL_ERR == elCreateFileEvent(server.el, cfd, EL_READABLE, readQueryFromClient, c)) {
        // todo free something
        return;
    }

    c->querybuf = NULL;
    c->buflen = 0;
    c->bufcap = 0;
    c->multilen = 0;
    c->bulklen = -1;
    c->argv = NULL;
    c->argc = 0;

    c->flag = 0;

    c->fd = cfd;
    c->reqtype = 0;
    selectDb(c, 0);
    c->cmd = NULL;
    c->bufpos = 0;
    c->sentlen = 0;
    c->reply = listCreate();
    listSetFreeMethod(c->reply, zfree);
    c->reply_bytes = 0ll;

    listAddNodeTail(server.client_list, c);
    c->client_list_node = listLast(server.client_list);

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
                addReplyError(c, errmsg);
                setProtocolError(c);
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
            addReplyError(c, errmsg);
            setProtocolError(c);
            return RESP_PROCESS_ERR;
        }

        pos+=newline-c->querybuf+2;

        c->multilen = value;

        if (c->multilen) {
            if(c->argv) zfree(c->argv);
            c->argv = zmalloc(sizeof(robj*) * c->multilen);
        }
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
                    addReplyError(c, errmsg);
                    setProtocolError(c);
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
                addReplyError(c, errmsg);
                setProtocolError(c);
                return RESP_PROCESS_ERR;
            }

            pos+=(newline-(c->querybuf+pos)) + 2;

            c->bulklen = value;

        }

        // not enough data to read
        if (c->bulklen > (c->buflen-pos) + 2) {
            break;
        } else {
            char *str = zmalloc(sizeof(char) * (c->bulklen + 1));
            memcpy(str, c->querybuf+pos, c->bulklen);
            str[c->bulklen] = '\0';
            c->argv[c->argc++] = createStringObject(str, strlen(str));
            zfree(str);
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

int processInlineBuffer(client *c) {

    char *newline = strchr(c->querybuf, '\n');
    size_t nread;
    if (newline == NULL) {
        if (c->buflen >= RESP_PROTO_MAX_INLINE_SEG) {
            addReplyError(c, "protocol read too big");
            setProtocolError(c);
        }
        return RESP_PROCESS_ERR;
    }

    nread = newline - c->querybuf + 1;

    if (*(newline-1) == '\r') {
        newline--;
    }

    size_t auxlen = newline-c->querybuf;

    char aux[auxlen+1];
    memcpy(aux, c->querybuf, auxlen);
    aux[auxlen] = '\0';

    int argc = 0;
    char **vector = stringsplitargs(aux, &argc);
    if (vector == NULL) {
        addReplyError(c, "protocol read too big");
        setProtocolError(c);
        return RESP_PROCESS_ERR;
    }

    if (argc) {
        if (c->argv) zfree(c->argv);
        c->argc = argc;
        c->argv = zmalloc(sizeof(robj*) * argc);
    }

    for (int i=0; i<argc; i++) {
        c->argv[i] = createObject(REDIS_OBJECT_STRING, sdsnewlen(vector[i], strlen(vector[i])));
        zfree(vector[i]);
    }
    zfree(vector);

    memmove(c->querybuf, c->querybuf+nread, c->buflen - nread);
    c->buflen-=nread;
    return RESP_PROCESS_OK;

}

int clientHasPendingWrites(client *c) {
    return c->bufpos > 0 || listLength(c->reply);
}


int writeToClient(client *c, int handler_installed) {

    size_t totwritten;
    size_t nwritten;
    while (clientHasPendingWrites(c)) {

        if (c->bufpos) {
            nwritten = write(c->fd, c->buf + c->sentlen, c->bufpos - c->sentlen);
            if (nwritten <= 0) break;
            c->sentlen+=nwritten;
            if (c->sentlen == c->bufpos) {
                c->bufpos = 0;
                c->sentlen = 0;
            }

        } else {
            listNode *node = listFirst(c->reply);
            char *s = listNodeValue(node);
            size_t slen = strlen(s);
            if (slen == 0) {
                listDelNode(c->reply, node);
                continue;
            }

            nwritten = write(c->fd, s+c->sentlen, slen - c->sentlen);
            if (nwritten <= 0) break;

            c->sentlen+=nwritten;
            c->reply_bytes-= nwritten;
            if (c->sentlen == slen) {
                c->sentlen = 0;
                listDelNode(c->reply, node);
            }
        }

        totwritten+=nwritten;
    }

    if (nwritten <= 0) {
        if (errno == EAGAIN) {
            nwritten = 0;
        } else {
            freeClient(c);
            return C_ERR;
        }
    }

    if (!clientHasPendingWrites(c)) {

        if (handler_installed) {
            if (elGetFileEvent(server.el, c->fd, EL_WRITABLE))
                if (elDeleteFileEvent(server.el, c->fd, EL_WRITABLE) == EL_ERR) {
                    freeClient(c);
                    return C_ERR;
                }
        }


        if (c->flag & CLIENT_CLOSE_AFTER_REPLY) {
            freeClient(c);
            return C_ERR;
        }

    }

    return C_OK;

}

void sendClientData (struct eventLoop *el, int fd, int mask, void *clientData) {
    client *c = (client*)clientData;
    writeToClient(c, 1);
}

void handleClientsPendingWrite(void) {

    listIter li;
    listNode *ln;
    list *l = server.client_pending_writes;
    listRewind(l, &li);
    while ((ln=listNext(&li))) {

        client *c = listNodeValue(ln);
        c->flag &= ~CLIENT_PENDING_WRITE;
        listDelNode(l, ln);

        // return C_ERR if client has been freed, return C_OK while client still valid
        if (writeToClient(c, 0) == C_ERR) continue;

        if (clientHasPendingWrites(c)) {

            if (elGetFileEvent(server.el, c->fd, EL_WRITABLE) == 0) {
                if (elCreateFileEvent(server.el, c->fd, EL_WRITABLE, sendClientData, c) != EL_OK) {
                    freeClientAsync(c);
                }
            }
        }
    }

}

void unlinkClient(client *c) {

    elDeleteFileEvent(server.el, c->fd, EL_WRITABLE);
    elDeleteFileEvent(server.el, c->fd, EL_READABLE);

    if (c->argc > 0) {
        for (int j = 0; j < c->argc; j++) {
            robj *obj = c->argv[j];
            decrRefCount(obj);
        }
        zfree(c->argv);
        c->argv = NULL;
    }

    close(c->fd);
    c->fd = -1;
}

void resetClient(client *c) {

    c->cmd = NULL;

    if (c->argc) {
        for (int j=0; j<c->argc; j++) {
            decrRefCount(c->argv[j]);
        }
        c->argc = 0;
        zfree(c->argv);
        c->argv = NULL;
    }

    c->reqtype = 0;
    c->multilen = 0;
    c->bulklen = -1;
}

void freeClient(client *c) {

    fprintf(stdout, "free client, c=%p\r\n", c);

    if (c->querybuf) {
        zfree(c->querybuf);
        c->querybuf = NULL;
    }


    listRelease(c->reply);
    c->reply = NULL;

    if (c->fd != -1) {
        unlinkClient(c);
    }

    if (c->flag & CLIENT_CLOSE_ASAP) {
        c->flag &= ~CLIENT_CLOSE_ASAP;
        listDelNode(server.client_close_list, c->client_close_node);
    }

    if (c->flag & CLIENT_PENDING_WRITE) {
        c->flag &= ~CLIENT_PENDING_WRITE;
        listNode *ln = listSearchKey(server.client_pending_writes, c);
        listDelNode(server.client_pending_writes, ln);
    }

    listDelNode(server.client_list, c->client_list_node);
}

void freeClientAsync(client *c) {
    if (c->flag & CLIENT_CLOSE_ASAP) return;
    c->flag |= CLIENT_CLOSE_ASAP;
    listAddNodeTail(server.client_close_list, c);
    c->client_close_node = listLast(server.client_close_list);

}


void freeClientInFreeQueueAsync(void) {
    listNode *ln = listFirst(server.client_close_list);
    listNode *next;
    while (ln) {
        client *c = (client*)ln->value;
        next = ln->next;
        freeClient(c);
        ln = next;
    }
}



