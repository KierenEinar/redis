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
#include "crc.h"
#include "dict.h"
#include "ziplist.h"
#include "quicklist.h"
#define LRUBITS 24

// define networking
#define SERVER_NETWORK_ERR_LEN 255
#define DEFAULT_BACKLOG 511
#define DEFAULT_BIND_PORT 6379
#define DEFAULT_LIST_FILL_FACTOR -2
#define SERVER_NETWORK_IP_FD_LEN 16

// define result
#define C_OK 1
#define C_ERR -1

#define REDIS_WAIT_RESOLUTION 10

#define REDIS_THREAD_STACK_SIZE 1024 * 1024 * 4

// proto read
#define PROTO_REQ_INLINE 1
#define PROTO_REQ_MULTI 2
#define PROTO_NREAD     (1024 * 16)

// proto reply
#define PROTO_REPLY_CHUNK_BYTES (1024 * 16)

// client flag
#define CLIENT_PENDING_WRITE (1 << 0)
#define CLIENT_CLOSE_ASAP (1 << 1)
#define CLIENT_CLOSE_AFTER_REPLY (1 << 2)
#define CLIENT_BLOCKED (1 << 3)
// server cron period
#define SERVER_CRON_PERIOD_MS 1

// redis object encoding
#define REDIS_ENCODING_RAW 1
#define REDIS_ENCODING_EMBED 2
#define REDIS_ENCODING_INT 3
#define REDIS_ENCODING_LIST 4

// redis object type
#define REDIS_OBJECT_STRING 1
#define REDIS_OBJECT_LIST   2

#define LIST_HEAD 0
#define LIST_TAIL 1

// redis share object ref_count
#define REDIS_SHARED_OBJECT_REF INT_MAX

#define REDIS_DEFAULT_DB_NUM 16

// -------- define expire unit ---------
#define UNIT_SECONDS 1
#define UNIT_MILLISECONDS 2

typedef struct redisObject {
    unsigned type:4;
    unsigned encoding:4;
    unsigned lru:LRUBITS;
    int refcount;
    void *ptr;
}robj;

typedef struct redisDb {
    dict* dict;
    dict* expires;
    dict *blocking_keys;
    dict *ready_keys;
}redisDb;

typedef struct blockingStates {
    long long timeout;
    dict* blocking_keys;

}blockingStates;

typedef struct client {
    char err[255];
    char *querybuf;
    size_t buflen;
    size_t bufcap;
    long multilen;
    long bulklen;
    robj **argv; // array of string
    long argvlen;
    int argc;

    int reqtype; // protocol is inline or multibulk
    int fd;

    unsigned long long flag; // client flag

    // reply list
    list *reply;
    unsigned long long reply_bytes;

    off_t sentlen;

    // reply
    off_t bufpos;
    char  buf[PROTO_REPLY_CHUNK_BYTES];

    listNode *client_list_node;
    listNode *client_close_node;

    redisDb *db;

    struct redisCommand *cmd;

    blockingStates bpop;

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

    list *client_pending_writes; // client pending write list

    list *client_list; // entire client node list
    list *client_close_list; // for the close asap client list

    redisDb *dbs;
    int dbnum;

    list *ready_keys;

    dict *commands;
    int list_fill_factor;

}redisServer;


#define OBJ_SHARED_INTEGERS 10000
#define OBJ_BULK_LEN_SIZE 32
#define OBJ_SHARED_REFCOUNT INT_MAX
struct redisSharedObject {
    robj *crlf, *ok, *syntaxerr, *nullbulk, *wrongtypeerr,
    *integers[OBJ_SHARED_INTEGERS],
    *mbulkhdr[OBJ_BULK_LEN_SIZE],
    *bulkhdr[OBJ_BULK_LEN_SIZE], *czero, *cone;
};

extern struct redisServer server;
extern struct redisSharedObject shared;
extern struct dictType dbDictType;
extern struct dictType keyptrDictType;
extern struct dictType objectKeyValueListDictType;
extern struct dictType objectKeyValuePtrDictType;
// --------------mstime---------------
typedef int64_t mstime_t;
mstime_t mstime();


// -------------dictType functions------------
int dictSdsCompare(const void *ptr1, const void *ptr2);
int dictSdsCaseCompare(const void *ptr1, const void *ptr2);

//--------------redisObject public method ---------------

// note: caller should call decrRefCount to release the ref count of robj using create* family

// create string type robj, callers needs to specify the encoding, the robj ref count set to 1.
// it is responsibility that caller should use decrRefCount to decr the ref count.
robj* createObject(int type, void *ptr);

// create string type robj, s is an embstr with robj, we suggest that is better
// use emstr when slen less or equal than 39.
// why 39, sdshdr is 8bytes, robj is 16bytes, so 64bytes - 24bytes - 1bytes('\0'), is 39.
robj* createEmbeddedStringObject(const char *s, size_t len);

// create string type robj, s is an raw string with robj, which robj->ptr = s ptr
robj* createRawStringObject(const char *s, size_t len);

// create a string type object, the encoding is raw or embstr
robj* createStringObject(const char *s, size_t len);

// create a string type object, the encoding is INT, robj ptr is the value.
robj* createStringObjectFromLongLong(long long value);

// create a list type object
robj* createListTypeObject();

// incr the refcount, will do nothing when o is a shared object.
void incrRefCount(robj *o);

// decr the refcount, will do nothing when o is a shared object, and free the object when ref count is 0.
void decrRefCount(robj *o);

// free the string type robj, only work on encoding raw string.
void freeStringObject(robj *o);

// update the ref count to specify this object is a share object.
void makeObjectShared(robj *o);

// try the best encoding to the robj, i.e. raw => embstr when len less than or eq 39,
// encoding raw or embstr when value is a integer.
robj* tryObjectEncoding(robj *obj);

// return 1 if redis object is encoding sds (embstr or raw format), otherwise return 0.
int sdsEncodedObject(robj *r);

// decoded the object, if o encoding is raw OR embstr, just incr the ref count, int encoding will try to encoding raw or emstr.
robj* getDecodedObject(robj *o);

// get long from redis object, obj must be encoding raw or embstr.
int getLongLongFromObject(robj *obj, long long *target);

// get long from redis object, reply on failed.
int getLongLongFromObjectOrReply(robj *obj, long long *target, client *c, robj *reply);

// create the golbal shared object
void createSharedObject(void);

// check key type matchs the giving param type, return 1 match, otherwise 0 return.
int checkType(robj *key, int type);

// get timeout
int getTimeoutFromObjectOrReply(client *c, robj* argv, int unit, long long *timeout, robj *reply);

//-------------- redis command process prototype -----------
void populateCommandTable(void);

// lookup command by redis object, should be the string type object.
struct redisCommand* lookupCommand(robj *o);

// getCommand get the value from kv, if key exists but not string type, client will receive wrong type error.
// as side effect, key will delete when go expire, client will receive nullbulk.
void getCommand(client *c);

// setCommand set the kv,
// set key value [EX EXPIRES] [PX milliseconds] [NX|XX]
void setCommand(client *c);

// utils for redis command prototype
// generic get command
int getGenericCommand(client *c);

// generic set command
int setGenericCommand(client *c, robj *key, robj *value, int flags, robj *expires, int unit, robj *ok_reply, robj *abort_reply);

// set multi key value
void msetCommand(client *c);

// get multi key value
void mgetCommand(client *c);

// set multi key value generic command
int msetGenericCommand(client *c, int nx);

// client use which db.
void selectDb(client *c, int id);

// get the key ttl
void ttlCommand(client *c);

// get the key pttl
void pttlCommand(client *c);

// push an element into head.
void lpushCommand(client *c);

// push an element into head.
void rpushCommand(client *c);

// pop an element from head.
void lpopCommand(client *c);

// pop an element from tail.
void rpopCommand(client *c);

// for blpop, brpop
void blockingGenericCommand(client *c, int where);

void blockForKeys(client *c, robj **argv, int argc, long long timeout);

// execute the command.
void call(client *c);

// ----------- command utils -------------

// delete the key from expires set if exists, but logically has been expired.
// return 1 if key remove from expires, 0 key not exists.
int expireIfNeed(client *c, const robj *key);

// lookup a key from select db, null received when key not exists or has been expired.
// as a side effect, if key exists but logical expired, will delete by using dbSyncDelete or dbAsyncDelete.
robj *lookupKeyRead(client *c, const robj *key);

// lookup a key from select db, null received when key not exists or has been expired.
// as a side effect, if key exists but logical expired, will delete by using dbSyncDelete or dbAsyncDelete.
robj *lookupKeyWrite(client *c, const robj *key);

// lookup a key from select db, null received when key not exists or has been expired.
// as a side effect, if key exists but logical expired, will delete by using dbSyncDelete or dbAsyncDelete.
// if key not exists or expired, reply will send to client if is not null.
robj *lookupKeyReadOrReply(client *c, robj *key, robj *reply);

// redis command prototype
typedef void redisCommandProc(client *c);

struct redisCommand {
    char *name;
    redisCommandProc *proc;
    int arity;
};

// ------------ db prototype ----------

// set key value
void setKey(client *c, robj *key, robj *value);

// set key expire
void setExpire(client *c, robj *key, long long expire);

// add db key value
void dbAdd(client *c, robj *key, robj *value);

// replace db key value
void dbReplace(client *c, robj *key, robj *value);

// remove db expire key
void removeExpire(client *c, robj *key);

// get the key expire milliseconds
long long getExpire(redisDb *db, robj *key);

// list prototype

// push command, lpush or rpush, where -> LIST_HEAD or LIST_TAIL.
void pushGenericCommand(client *c, int where);

// pop command, lpop or rpop, where -> LIST_HEAD or LIST_TAIL
void popGenericCommand(client *c, int where);

// push.
void listTypePush(robj *subject, robj *value, int where);

// pop.
robj* listTypePop(robj *subject, int where);

// pop saver.
void* listPopSaver(void *data, unsigned int size);

// list len.
unsigned int listTypeLen(robj *lobj);

//-------------syncio-------------------
size_t syncRead(int fd, char *ptr, size_t size, long long timeout);

//-------------networking----------------
void acceptTcpHandler(struct eventLoop *el, int fd, int mask, void *clientData);
void acceptCommandHandler(int cfd, char *ip, int port);

int processMultiBulkBuffer(client *client);
int processInlineBuffer(client *client);

int clientHasPendingWrites(client *c);
int writeToClient(client *client, int handler_installed);
void handleClientsPendingWrite(void);

//-------------reply--------------------
void addReply(client *c, robj *r);
// reply bulk string to client.
void addReplyBulk(client *c, robj *r);
// add bulk len to reply prefix
void addReplyBulkLen(client *c, robj *r);
// add multi bulk len header to reply
void addReplyMultiBulkLen(client *c, long long len);
// reply long long to client
void addReplyLongLong(client *c, long long value);
// add reply length header.
void addReplyLongLongPrefix(client *c, long long value, char prefix);
void addReplyString(client *c, const char *str, size_t len);
void addReplyError(client *c, const char *str);
void addReplyErrorLength(client *c, const char *str, size_t len);
void setProtocolError(client *c);

// ------------redis client--------------

// reset the client so it can process command again.
void resetClient(client *c);

void freeClient(client *c);
void freeClientAsync(client *c);
void freeClientInFreeQueueAsync(void);

//-------------cron job-----------------
long long serverCron(struct eventLoop *el, int id, void *clientData);

// ------------process command -------------
int processCommand(client *c);



#endif //REDIS_SERVER_H
