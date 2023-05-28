//
// Created by kieren jiang on 2023/5/22.
//

#ifndef REDIS_RIO_H
#define REDIS_RIO_H

#include <stdio.h>
#include "sds.h"

typedef struct _rio {
    /**
     * since this functions do not tolerate the short writes or reads
     * the return value is simplified to return error on zero, non zero on complete success.
     * */
    size_t (*write)(struct _rio *, const char *s, size_t t);
    size_t (*read) (struct _rio *, char *s, size_t t);
    off_t (*tell) (struct _rio *);
    size_t (*flush) (struct _rio *);

    size_t processed_bytes;
    size_t max_processing_chunk;


    union {
        // in memory pointer target
        struct {
             sds ptr;
             off_t pos;
        } buffer;

        // stdio file pointer target
        struct {
            FILE *ptr;
            off_t buffered; // bytes written since last fsync.
            off_t autosync; // fsync after 'autosync' bytes write
        } file;
    }io;

}rio;


static inline size_t rioWrite(rio *rio, const char *s, size_t len) {

    size_t bytes_to_write = rio->max_processing_chunk > len ? len : rio->max_processing_chunk;
    while (len) {
        size_t n = rio->write(rio, s, bytes_to_write);
        if (n == 0) {
            return 0;
        }
        len-=n;
        s+=len;
        rio->processed_bytes+=len;
    }
    return 1;
}

static inline size_t rioRead(rio *rio, char *s, size_t len) {

    size_t bytes_to_read = rio->max_processing_chunk > len ? len : rio->max_processing_chunk;

    while (len) {
        size_t n = rio->read(rio, s, bytes_to_read);
        if (n == 0) {
            return 0;
        }
        rio->processed_bytes+=n;
        len-=n;
        s+=n;
    }
    return 1;
}

static inline off_t rioTell(rio *rio) {
    return rio->tell(rio);
}

static inline off_t rioFlush(rio *rio) {
    return rio->flush(rio);
}

//-----------------------init --------------------

void rioInitWithFile(rio *, FILE *fp);
void rioInitWithBuffer(rio *, sds s);

// --------------generic functions -----------------
//void rioSetAutoSync(rio *, off_t autosync);
//
//// -------------higher level interface ----------------
//size_t rioWriteBulkCount(rio *, char prefix, long count);
//size_t rioWriteBulkString(rio *, const char *s, size_t len);
//size_t rioWriteBulkLongLong(rio *, long long value);
//struct redisObject;
//size_t rioWriteBulkObject(rio *, struct redisObject *obj);

#endif //REDIS_RIO_H
