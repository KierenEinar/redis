//
// Created by kieren jiang on 2023/2/20.
//

#ifndef REDIS_SERVER_H
#define REDIS_SERVER_H

#include <limits.h>
#include <stdlib.h>
#include <time.h>
#include <stdio.h>
#include <unistd.h>
#include <stdlib.h>
#include <sys/time.h>
#include <sys/types.h>
#include <memory.h>
#include <string.h>
#include <pthread.h>
#include <signal.h>

#include "dict.h"
#include "sds.h"
#include "varint.h"
#include "utils.h"
#include "zmalloc.h"
#include "el.h"
#include "bio.h"
#include "config.h"
#include "adlist.h"
#include "rio.h"

#define LRUBITS 24
// define obj type
#define OBJECT_STRING 1

// define obj encoding
#define OBJECT_ENCODING_RAW 1
#define OBJECT_ENCODING_INT 2
#define OBJECT_ENCODING_EMBSTR 3

#define STRING_INT_LIMIT_LEN 20
#define EMBSTR_LEN_LIMIT 39

// define protocol
#define PROTO_IO_BUF_SIZE 1024 * 16
#define PROTO_MAX_INLINE_SIZE 1024 * 32
#define PROTO_MAX_ARG_SIZE 1024 * 32
#define PROTO_MAX_BULK_SIZE 1024 * 1024 * 512
#define PROTO_REQ_MULTI_BULK 1
#define PROTO_REQ_INLINE 2

// define result
#define C_OK 1
#define C_ERR -1

#define REDIS_THREAD_STACK_SIZE 1024 * 1024 * 4

typedef struct redisObject {
    unsigned type:4;
    unsigned encoding:4;
    unsigned lru:LRUBITS;
    int refcount;
    void *ptr;
}robj;

typedef struct redisDb {
    dict *dict;
    dict *expires;
}db;

typedef void redisCommandProc(struct client* c);

typedef struct redisCommand {
    char *name;
    int arty;
    redisCommandProc *proc;
}redisCommand;

typedef struct client {

    int fd;

    sds query_buf;
    int reqtype;
    long multibulklen;
    long long bulklen;

    long argc;
    robj **argv;

    db *db;
    redisCommand *cmd;

    time_t ctime;
}client;

typedef struct redisServer {
    db *db; // arrays of redisdb
    int db_nums; // nums of db, set default 16
    client **client; // arrays of client ptr

    // event loop
    eventLoop *el;

    time_t unix_time;

};

#define OBJ_SHARED_INTEGERS 10000
#define OBJ_SHARED_REFCOUNT INT_MAX
struct sharedObject {
    robj *integers[OBJ_SHARED_INTEGERS];
};


uint64_t dictSdsHash(const void *key);
int dictSdsComparer(const void *key1, const void *key2);
void dictSdsDestructor(void *val);
void dictObjectDestructor(void *val);

extern struct redisServer server;
extern struct sharedObject shared;

// --------------mstime---------------
typedef int64_t mstime_t;
mstime_t mstime();

//--------------redisObject public method ---------------
robj* createObject(int type, void *ptr);
robj* createEmbeddedStringObject(const char *s, size_t len);
robj* createRawStringObject(const char *s, size_t len);
robj* createStringObject(const char *s, size_t len);
robj* createStringObjectFromLongLong(long long value);

void decrRefCount(robj *o);
void freeStringObject(robj *o);
void makeObjectShared(robj *o);
robj* tryObjectEncoding(robj *obj);
robj* getDecodedObject(robj *o);
int getLongLongFromObject(robj *obj, long long *target);
int getLongFromObject(robj *obj, long *target);

static inline void initStaticStringObject(robj *o, void *ptr) {
    o->refcount = 1;
    o->type = OBJECT_STRING;
    o->encoding = OBJECT_ENCODING_RAW;
    o->ptr = ptr;
}

//----------------client method----------------------
void readQueryFromClient(eventLoop *el, int fd, int mask, void *clientData);
void processInputBuffer(client *c);
int processMultiBulkBuffer(client *c);


//-------------syncio-------------------
size_t syncRead(int fd, char *ptr, size_t size, long long timeout);

#endif //REDIS_SERVER_H
