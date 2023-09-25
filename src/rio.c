//
// Created by kieren jiang on 2023/5/22.
//

#include "rio.h"
#include "server.h"
static size_t rioBufferRead(rio *rio, char *s, size_t len) {
    if (sdslen(rio->io.buffer.ptr) - rio->io.buffer.pos < len)
        return 0;

    memcpy(s, rio->io.buffer.ptr + rio->io.buffer.pos, len);
    rio->io.buffer.pos+=len;
    return 1;
}


static size_t rioBufferWrite(rio *rio, const char *s, size_t len) {

    if (sdslen(rio->io.buffer.ptr) < len)
        rio->io.buffer.ptr = sdsMakeRoomFor(rio->io.buffer.ptr, len);

    rio->io.buffer.ptr = sdscatlen(rio->io.buffer.ptr, s, len);
    rio->io.buffer.pos+=(off_t)len;
    return 1;
}


off_t rioBufferTell(rio *rio) {
    return rio->io.buffer.pos;
}

size_t rioBufferFlush(rio *rio) {
    return 1;
}

const static rio rioBuffer = {
    rioBufferWrite,
    rioBufferRead,
    rioBufferTell,
    rioBufferFlush,
    0,
    0,
    {{NULL, 0}}
};


void rioInitWithBuffer(rio *r, sds s) {
    *r = rioBuffer;
    r->io.buffer.ptr = s;
    r->io.buffer.pos = 0;
}


static size_t rioFileRead(rio *rio, char *s, size_t len) {
    return fread(rio->io.file.ptr, len, 1, NULL);
}


static size_t rioFileWrite(rio *rio, const char *s, size_t len) {

    if (fwrite(rio->io.file.ptr, len, 1, NULL) < 1)
        return 0;

    rio->io.file.buffered+=len;

    if (rio->io.file.autosync && rio->io.file.buffered >= rio->io.file.autosync) {
        fflush(rio->io.file.ptr);
        fsync(fileno(rio->io.file.ptr));
        rio->io.file.buffered = 0;
    }
    return 1;
}


off_t rioFileTell(rio *rio) {
    return ftello(rio->io.file.ptr);
}

size_t rioFileFlush(rio *rio) {
    return fflush(rio->io.file.ptr) == 0 ? 1: 0;
}

const static rio rioFile = {
        rioFileWrite,
        rioFileRead,
        rioFileTell,
        rioFileFlush,
        0,
        0,
        {{NULL, 0}}
};

void rioInitWithFile(rio *r, FILE *fp) {
    *r = rioFile;
    r->io.file.ptr = fp;
    r->io.file.buffered = 0;
}

void rioSetAutoSync(rio *r, off_t autosync) {
    r->io.file.autosync = autosync;
}

//size_t rioWriteBulkCount(rio *r, char prefix, long count) {
//
//    char tmp[128];
//    tmp[0] = prefix;
//    size_t clen = 1;
//    clen += ll2string(tmp+1, sizeof(tmp) - 1, (long long)(count));
//    tmp[clen++] = '\r';
//    tmp[clen++] = '\n';
//
//    if (rioWrite(r, tmp, clen) == 0) return 0;
//    return clen;
//}
//
//size_t rioWriteBulkString(rio *r, const char *s, size_t len) {
//
//    // $<bulklen>\r\n
//    // <bulkstr>\r\n
//
//    size_t nwritten;
//    if ((nwritten = rioWriteBulkCount(r, '$', len)) == 0) return 0;
//    if (len > 0 && rioWrite(r, s, len) == 0) return 0;
//    if (rioWrite(r, "\r\n", 2) == 0) return 0;
//    return nwritten + len + 2;
//}
//
//size_t rioWriteBulkLongLong(rio *r, long long value) {
//    char tmp[32];
//    size_t clen = ll2string(tmp, sizeof(tmp), value);
//    return rioWriteBulkString(r, tmp, clen);
//}
//
//size_t rioWriteBulkObject(rio *r, robj *obj) {
//
//    // avoid using getDecoded to help copy on write (
//    // we are often in a child process when this function called)
//
//    if (obj->encoding == OBJECT_ENCODING_INT) {
//        return rioWriteBulkLongLong(r, (long)obj->ptr);
//    } else if (sdsEncodedObject(obj)) {
//        return rioWriteBulkString(r, (sds)obj->ptr, sdslen(obj->ptr));
//    } else {
//        // todo server panic
//    }
//}
