//
// Created by kieren jiang on 2023/4/6.
//


#include "server.h"

int processMultiBulkBuffer(client *c) {

    long pos = 0;
    char *newline = NULL;
    int ok;
    int qblen = sdslen(c->query_buf);
    if (!c->multibulklen) {
        newline = strchr(c->query_buf, '\r');
        if (newline == NULL) {
            if (qblen > PROTO_MAX_INLINE_SIZE) {
                // multi count invalid
                // todo add reply error
                // todo set protocol error
            }
            return REDIS_ERR;
        }

        // buffer should contain \n
        if (newline - c->query_buf > qblen - 2) {
            return REDIS_ERR;
        }

        long long ll;
        ok = string2ll(c->query_buf+1, newline-(c->query_buf+1), &ll);
        if (!ok || ll > 1024 * 1024) {
            // todo add reply error
            // todo set protocol error
            return REDIS_ERR;
        }

        if (ll <= 0) {
            return REDIS_OK;
        }

        c->multibulklen = (long)ll;
        pos = newline - c->query_buf + 2;

        if (c->argv) zfree(c->argv);
        c->argv = zmalloc(sizeof(robj) * ll);
    }


    while (c->multibulklen) {

        if (c->bulklen == -1) {

            newline = strchr(c->query_buf + pos, '\r');
            if (newline == NULL) {
                if (sdslen(c->query_buf) > PROTO_MAX_INLINE_SIZE) {
                    // bulk count invalid
                    // todo add reply error
                    // todo set protocol error
                    return REDIS_ERR;
                }
                break;
            }

            // buffer should contain \n
            // e.g. *1\r\n$3\r\nabc\r\n
            if (newline - c->query_buf > qblen - 2) {
                break;
            }

            if (c->query_buf+pos != '$') {
                // todo add reply error
                // todo set protocol error
                return REDIS_ERR;
            }

            long long ll;
            ok = string2ll(c->query_buf + pos + 1, newline - (c->query_buf + pos + 1), &ll);
            if (!ok || ll < 0 || ll > PROTO_MAX_BULK_SIZE) {
                // todo add reply error
                // todo set protocol error
                return REDIS_ERR;
            }

            c->bulklen = ll;

            pos = newline - c->query_buf + 2;

            if (ll >= PROTO_MAX_ARG_SIZE) {
                sdsrange(c->query_buf, pos, -1);
                int qblen = sdslen(c->query_buf);

                if (qblen < c->bulklen + 2)
                    c->query_buf = sdsMakeRoomFor(c->query_buf, c->bulklen + 2);

                pos = 0;
            }

        }

        // not enough space
        if (sdslen(c->query_buf + pos) < c->bulklen + 2) {
            break;
        } else {

            if (pos == 0 && sdslen(c->query_buf) == c->bulklen+2 && c->bulklen >= PROTO_REQ_MULTI_BULK) {
                c->argv[c->argc++] = createObject(OBJECT_STRING, c->query_buf);
                sdsincrlen(c->query_buf, -2);
                c->query_buf = sdsnewlen(NULL, c->bulklen+2);
                sdsincrlen(c->query_buf, -(c->bulklen+2));
                pos = 0;
            } else {
                c->argv[c->argc++] = createStringObject(c->query_buf+pos, c->bulklen);
                pos += c->bulklen + 2;
            }

            c->bulklen = -1;
        }

        c->multibulklen--;

    }

    if (pos) sdsrange(c->query_buf, pos, -1);

    if (c->multibulklen == 0 ) return REDIS_OK;

    return REDIS_ERR;

}


void processInputBuffer(client *c) {

    while (c->query_buf) {

        if (!c->reqtype) {
            if (c->query_buf == '*') c->reqtype = PROTO_REQ_MULTI_BULK;
            else c->reqtype = PROTO_REQ_INLINE;
        }

        if (c->reqtype == PROTO_REQ_MULTI_BULK) {
            if (processMultiBulkBuffer(c) != REDIS_OK) {
                break;
            }
        } else {
            // todo processInlineBuffer
            break;
        }

        // todo process command
        if (c->argc == 0) {
            // todo reset client
        } else {
            //todo processCommand(c)
        }

    }

}



void readQueryFromClient(eventLoop *el, int fd, int mask, void *clientData) {

    client *c = clientData;

    size_t readlen = PROTO_IO_BUF_SIZE;

    if (c->reqtype == PROTO_REQ_MULTI_BULK && c->multibulklen && c->bulklen >= PROTO_MAX_ARG_SIZE) {
        size_t remaining = c->bulklen + 2 - sdslen(c->query_buf);
        if (remaining < readlen) readlen = remaining;
    }

    int qblen = sdslen(c->query_buf);

    c->query_buf = sdsMakeRoomFor(c->query_buf, readlen);

    size_t nread = read(fd, c->query_buf+qblen, readlen);

    if (nread == -1) {
        if (errno == EAGAIN) {
            return;
        }
        // read error
        // todo free client
    } else if (nread == 0) {
        // connection closed
        // todo free client
    } else {

    }

    sdsincrlen(c->query_buf, nread);

    processInputBuffer(c);
}
