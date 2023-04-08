//
// Created by kieren jiang on 2023/2/20.
//

#ifndef REDIS_SERVER_H
#define REDIS_SERVER_H

#include "dict.h"
#include "sds.h"
#include "varint.h"
#include "utils.h"
#include "zmalloc.h"
#include "el.h"

#include <limits.h>
#include <stdlib.h>
#include <time.h>
#include <stdio.h>
#include <unistd.h>
#include <stdlib.h>
#include <sys/time.h>
#include <memory.h>
#include <string.h>

extern struct redisObject;
extern struct redisDb;
extern struct client;
extern struct redisCmd;

#define LRUBITS 24
// define obj type
#define OBJECT_STRING 1

// define obj encoding
#define OBJECT_ENCODING_RAW 1
#define OBJECT_ENCODING_INT 2
#define OBJECT_ENCODING_EMBSTR 3

#define STRING_INT_LIMIT_LEN 20
#define EMBSTR_LEN_LIMIT 39

// define redis eviction strategy
#define MAXMEMORY_FLAG_LRU 1 << 0
#define MAXMEMORY_FLAG_LFU 1 << 1
#define MAXMEMORY_ALLKEYS 1 << 2

// define maxmemorry policy
#define MAXMEMORY_VOLATILE_LRU 0 << 8 | MAXMEMORY_FLAG_LRU
#define MAXMEMORY_VOLATILE_LFU 1 << 8 | MAXMEMORY_FLAG_LFU
#define MAXMEMORY_VOLATILE_TTL 2 << 8
#define MAXMEMORY_VOLATILE_RANDOM 3 << 8

#define MAXMEMORY_ALLKEYS_LRU 4 << 8 | MAXMEMORY_FLAG_LRU | MAXMEMORY_ALLKEYS
#define MAXMEMORY_ALLKEYS_LFU 5 << 8 | MAXMEMORY_FLAG_LFU | MAXMEMORY_ALLKEYS
#define MAXMEMORY_ALLKEYS_RANDOM 6 << 8 | MAXMEMORY_ALLKEYS
#define MAXMEMORY_NOEVICTION 7 << 8

#define CLOCK_RESOLUTION 1000
#define CLOCK_MAX ((1<<24) -1)

// define protocol
#define PROTO_IO_BUF_SIZE 1024 * 16
#define PROTO_MAX_INLINE_SIZE 1024 * 32
#define PROTO_MAX_ARG_SIZE 1024 * 32
#define PROTO_MAX_BULK_SIZE 1024 * 1024 * 512
#define PROTO_REQ_MULTI_BULK 1
#define PROTO_REQ_INLINE 2

// define result
#define REDIS_OK 1
#define REDIS_ERR -1

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

typedef struct redisCmd {
    char *name;
    int arty;
    redisCommandProc *proc;
}cmd;

typedef struct client {

    int fd;

    sds query_buf;
    int reqtype;
    long multibulklen;
    long long bulklen;

    long argc;
    robj **argv;

    db *db;
    cmd *cmd;

    time_t ctime;
}client;

typedef struct redisServer {
    db *db; // arrays of redisdb
    int db_nums; // nums of db, set default 16
    client **client; // arrays of client ptr
    unsigned long long maxmemorry; // max memorry
    int maxmemorry_policy; // max memorry policy, see define maxmemorry policy
    int maxmemorry_samples; // max memorry samples keys count each time
};


uint64_t dictSdsHash(const void *key);
int dictSdsComparer(const void *key1, const void *key2);
void dictSdsDestructor(void *val);
void dictObjectDestructor(void *val);

// global vars
extern dictType dbDictType;
extern struct redisServer server;

// --------------mstime---------------
typedef int64_t mstime_t;
mstime_t ms_now();

//--------------command-----------------
void setCommand(client *c);
void setGenericCommand(client *c, robj *key, robj *value, robj *expire, int unit, int flags);

//--------------redisObject public method ---------------
robj* createObject(int type, void *ptr);
robj* createEmbeddedStringObject(const char *s, size_t len);
robj* createRawStringObject(const char *s, size_t len);
robj* createStringObject(const char *s, size_t len);

void decrRefCount(robj *o);
void freeStringObject(robj *o);

robj* tryObjectEncoding(robj *obj);
int getLongLongFromObject(robj *obj, long long *target);
int getLongFromObject(robj *obj, long *target);

//----------------client method----------------------
void readQueryFromClient(eventLoop *el, int fd, int mask, void *clientData);
void processInputBuffer(client *c);
int processMultiBulkBuffer(client *c);

//-----------------db public method----------------------
robj* lookupKeyWrite(db *db, robj *key);
int expireIfNeeded(db *db, robj *key);

#define LOOKUP_NOTOUCH 1 << 0
#define LOOKUP_TOUCH 1 << 1
robj* lookupKey(db *db, robj *key, int flags);
int getExpire(db *db, robj *key, mstime_t *expireMilliSeconds);
int dbSyncDelete(db *db, robj *key);

void setKey(db *db, robj *key, robj *val);

int dbAdd(db *db, robj *key, robj *val);
int dbOverwrite(db *db, robj *key, robj *val);
#endif //REDIS_SERVER_H
