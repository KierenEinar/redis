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

#include "varint.h"
#include "utils.h"
#include "zmalloc.h"
#include "el.h"
#include "bio.h"
#include "config.h"
#include "rio.h"
#include "adlist.h"
#include "sds.h"

#define LRUBITS 24

// define result
#define C_OK 1
#define C_ERR -1

#define REDIS_WAIT_RESOLUTION 10

#define REDIS_THREAD_STACK_SIZE 1024 * 1024 * 4

typedef struct redisObject {
    unsigned type:4;
    unsigned encoding:4;
    unsigned lru:LRUBITS;
    int refcount;
    void *ptr;
}robj;

//typedef void redisCommandProc(struct client* c);
//
//typedef struct redisCommand {
//    char *name;
//    int arty;
//    redisCommandProc *proc;
//}redisCommand;


typedef struct redisServer {

    // event loop
    eventLoop *el;

    time_t unix_time;

}redisServer;

#define OBJ_SHARED_INTEGERS 10000
#define OBJ_SHARED_REFCOUNT INT_MAX
struct sharedObject {
    robj *integers[OBJ_SHARED_INTEGERS];
};

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


//-------------syncio-------------------
size_t syncRead(int fd, char *ptr, size_t size, long long timeout);

#endif //REDIS_SERVER_H
