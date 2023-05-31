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
#include <netdb.h>
#include <sys/socket.h>
#include <arpa/inet.h>
#include <stdarg.h>
#include <fcntl.h>

#include "varint.h"
#include "utils.h"
#include "zmalloc.h"
#include "el.h"
#include "bio.h"
#include "config.h"
#include "rio.h"
#include "adlist.h"
#include "sds.h"
#include "anet.h"

#define LRUBITS 24

// define networking
#define SERVER_NETWORK_ERR_LEN 255
#define DEFAULT_BACKLOG 511
#define DEFAULT_BIND_PORT 6379
#define SERVER_NETWORK_IP_FD_LEN 16

// define result
#define C_OK 1
#define C_ERR -1

#define REDIS_WAIT_RESOLUTION 10

#define REDIS_THREAD_STACK_SIZE 1024 * 1024 * 4

#define PROTO_REQ_INLINE 1
#define PROTO_REQ_MULTI 2
#define PROTO_NREAD     (1024 * 16)

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

typedef struct client {
    char err[255];
    char *querybuf;
    size_t buflen;
    size_t bufcap;
    long multilen;
    long bulklen;
    char **argv; // array of string
    long argvlen;
    int argc;

    int reqtype; // protocol is inline or multibulk

    int fd;
} client;


typedef struct redisServer {

    // event loop
    eventLoop *el;

    time_t unix_time;

    // networking
    int ipfd_count;
    int ipfd[SERVER_NETWORK_IP_FD_LEN];
    char neterr[SERVER_NETWORK_ERR_LEN];
    int port;
    int backlog;

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

//-------------networking----------------
void acceptTcpHandler(struct eventLoop *el, int fd, int mask, void *clientData);
void acceptCommandHandler(int cfd, char *ip, int port);

int processMultiBulkBuffer(client *client);
int processInlineBuffer(client *client);
#endif //REDIS_SERVER_H
