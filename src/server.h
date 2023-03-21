//
// Created by kieren jiang on 2023/2/20.
//

#ifndef REDIS_SERVER_H
#define REDIS_SERVER_H

#include "dict.h"
#include "sds.h"
#include "varint.h"
#include "utils.h"

#include <limits.h>
#include <stdlib.h>
#include <time.h>
#include <stdio.h>
#include <sys/time.h>

extern struct redisObj;
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
    sds query_buf;
    int argc;
    robj **argv;

    db *db;
    cmd *cmd;

    time_t ctime;
}client;

uint64_t dictSdsHash(const void *key);
int dictSdsComparer(const void *key1, const void *key2);
void dictSdsDestructor(void *val);
void dictObjectDestructor(void *val);

extern dictType dbDictType;

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

void decrRefCount(robj *o);
void freeStringObject(robj *o);

robj* tryObjectEncoding(robj *obj);
int getLongLongFromObject(robj *obj, long long *target);
int getLongFromObject(robj *obj, long *target);



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
