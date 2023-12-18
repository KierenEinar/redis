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
#include <netinet/tcp.h>
#include <arpa/inet.h>
#include <stdarg.h>
#include <fcntl.h>
#include <errno.h>
#include <sys/stat.h>

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
#define CONFIG_TCP_BACKLOG 511
#define CONFIG_BIND_PORT 6379
#define CONFIG_QUICKLIST_FILL_FACTOR (-2)
#define CONFIG_NETWORK_IP_FD_LEN 16

#define CONFIG_AOF_ON 1
#define CONFIG_AOF_FILENAME "appendonly.aof"
#define CONFIG_REPL_RUNID_LEN 40
#define CONFIG_REPL_EOFMARK_LEN 40
#define CONFIG_REPL_BACKLOG_SIZE (1024 * 1024 * 10) // 10m
#define CONFIG_REPL_DISKLESS_SYNC 1 // default set to enable socket.
#define CONFIG_REPL_PING_PERIOD 10 // each 10 seconds send ping to our slaves.
#define CONFIG_REPL_TIMEOUT 60 // 60 seconds timed out if no data nor ack received.
#define CONFIG_REPL_BACKLOG_TIMEOUT (60 * 60) // 1 hour for repl backlog if there is no slaves.
#define CONFIG_REPL_DISKLESS_SYNC_DELAY 10 // 10 seconds for delay
#define CONFIG_REPL_SEND_TIMEOUT 10 // 10 seconds for replicate send time out.
#define CONFIG_REPL_TRANSFER_SYNC_LEN (1024 * 1024 * 8) // 8m
#define CONFIG_SLAVE_IDLE 60
#define LIST_ITER_DIR_FORWARD 1
#define LIST_ITER_DIR_BACKWARD 2

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
#define CLIENT_PUBSUB (1 << 4)
#define CLIENT_MULTI (1 << 5)
#define CLIENT_DIRTY_EXEC (1 << 6)
#define CLIENT_CAS_EXEC (1 << 7)
#define CLIENT_FAKE (1 << 8)
#define CLIENT_MASTER (1 << 9)
#define CLIENT_SLAVE (1 << 10)
#define CLIENT_FORCE_REPLY_MASTER (1<<11)
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

// ------- propagate flags ------------
#define PROPAGATE_CMD_NONE 0
#define PROPAGATE_CMD_AOF 1
#define PROPAGATE_CMD_REPL 2
#define PROPAGATE_CMD_FULL (PROPAGATE_CMD_AOF | PROPAGATE_CMD_REPL)

// ------------ AOF POLICY-------------
#define AOF_FSYNC_NO  0
#define AOF_FSYNC_ALWAYS 1
#define AOF_FSYNC_EVERYSEC 2

// ------------ AOF STATE --------------
#define AOF_OFF 0
#define AOF_ON 1
#define AOF_WAIT_REWRITE 2

// ------------ AOF TYPE ---------------
#define AOF_SAVE_TYPE_NONE 0
#define AOF_SAVE_TYPE_RW 1
#define AOF_SAVE_TYPE_REPLICATE_DISK 2
#define AOF_SAVE_TYPE_REPLICATE_SOCKET 3

//------------ AOF REWRITE --------------
#define AOF_REWRITE_BLOCK_SIZE (1024 * 1024 * 10)
#define AOF_FWRITE_BLOCK_SIZE (1024 * 4)

// ----------- AOF SYNC REPL---------------
#define AOF_PROTO_REPL_WRITE_SIZE (1024 * 16)

// ----------- REPL_STATE -----------
#define REPL_STATE_NONE 0
#define REPL_STATE_CONNECT 1
#define REPL_STATE_CONNECTING 2
// states must be ordered
// handshake stage
#define REPL_STATE_RECEIVE_PONG 3
#define REPL_STATE_SEND_AUTH 4
#define REPL_STATE_RECEIVE_AUTH 5
#define REPL_STATE_SEND_PORT 6
#define REPL_STATE_RECEIVE_PORT 7
#define REPL_STATE_SEND_IPADDRESS 8
#define REPL_STATE_RECEIVE_IPADDRESS 9
#define REPL_STATE_SEND_CAPA 10
#define REPL_STATE_RECEIVE_CAPA 11
#define REPL_STATE_SEND_PSYNC 12
#define REPL_STATE_RECEIVE_PSYNC 13

// data transfer stage
#define REPL_STATE_TRANSFER 14
#define REPL_STATE_CONNECTED 15

// CLIENT SLAVE STATE
#define SLAVE_STATE_WAIT_BGSAVE_START 6
#define SLAVE_STATE_WAIT_BGSAVE_END 7
#define SLAVE_STATE_ONLINE 8

// REPL CAPA
#define REPL_CAPA_NONE 0
#define REPL_CAPA_PSYNC2 1
#define REPL_CAPA_EOF 2

// ------------- REPL_PARTIAL-------------
#define PSYNC_WRITE_ERROR 1
#define PSYNC_WAIT_REPLY 2
#define PSYNC_TRY_LATER 3
#define PSYNC_FULL_RESYNC 4
#define PSYNC_CONTINUE 5
#define PSYNC_NOT_SUPPORT 6
// ------------debug --------------
#define debug(...) do { \
     printf("[DEBUG] ");  \
     printf(__VA_ARGS__); \
     printf("\n");      \
}while(0);

typedef struct redisObject {
    unsigned type:4;
    unsigned encoding:4;
    unsigned lru:LRUBITS;
    int refcount;
    void *ptr;
}robj;

typedef struct redisDb {
    int id;
    dict* dict;
    dict* expires;
    dict *blocking_keys;
    dict *ready_keys;
    dict *watch_keys;
}redisDb;

typedef struct blockingStates {
    long long timeout;
    dict* blocking_keys;

}blockingStates;

typedef struct multiCmd {
    robj **argv;
    int argc;
    struct redisCommand *cmd;
}multiCmd;

typedef struct multiStates {
    multiCmd *multi_cmds;
    int count;
}multiStates;

typedef struct client {

    unsigned long long id;

    char err[255];
    char *querybuf;
    size_t buflen;
    size_t bufcap;
    long multilen;
    long bulklen;
    robj **argv; // array of string
    int argc;

    int reqtype; // protocol is inline or multibulk
    int fd;
    time_t lastinteraction;
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

    // b[rl]pop
    blockingStates bpop;
    // multi
    multiStates mstate;
    list *watch_keys;

    // pubsub
    dict *pubsub_channels;
    list *pubsub_patterns;

    // replicate
    // master
    char master_replid[CONFIG_REPL_RUNID_LEN + 1];
    off_t repl_offset; // total bug len that master send, and we process it.
    off_t repl_read_offset; // total buf len that master send.

    // slave
    int repl_state;
    off_t psync_initial_offset; /* FULLRESYNC reply offset other slaves
                                       copying this slave output buffer
                                       should use. */
    int repl_put_online_ack;
    time_t repl_last_ack;
    int repl_capa;

} client;

typedef struct readyList {
    redisDb *db;
    robj *key;
}readyList;

typedef struct redisServer {

    // clients
    unsigned long long nextid;

    // event loop
    eventLoop *el;

    time_t unix_time;

    // networking
    int ipfd_count;
    int ipfd[CONFIG_NETWORK_IP_FD_LEN];
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

    dict *pubsub_channels;
    list *pubsub_patterns;

    // aof
    int aof_seldb;
    sds aof_buf;
    unsigned long long dirty;
    long long aof_postponed_start;
    off_t aof_update_size;
    int aof_fd;
    int aof_fsync;
    char *aof_filename;
    time_t aof_last_fsync;
    int aof_off;
    int aof_state;
    int aof_save_type;
    int aof_rw_schedule;

    int loading;
    time_t loading_time;
    off_t aof_loaded_bytes;
    off_t aof_loading_total_bytes;
    int aof_loaded_truncated;
    int aof_child_pid;
    list *aof_rw_block_list;
    int aof_pipe_read_data_from_parent;
    int aof_pipe_write_data_to_child;
    int aof_pipe_read_ack_from_child;
    int aof_pipe_write_ack_to_parent;
    int aof_pipe_read_ack_from_parent;
    int aof_pipe_write_ack_to_child;
    int aof_stop_sending_diff;
    sds aof_child_diff;


    // replicate master
    list *slaves;      // slaves list

    off_t master_repl_offset; // current master replicate offset.

    // circular backlog.
    char *repl_backlog;
    // backlog circular arr info
    long long repl_backlog_size; // backlog size.
    long long repl_backlog_idx;  // backlog next write offset, [0, backlog size)
    long long repl_backlog_histlen; // backlog valid read len, [0, backlog size)

    // backlog virtual arr info
    long long repl_backlog_off;  // backlog total offset, using for psync compare with slave's offset.

    int repl_seldbid;
    int repl_send_ping_period;
    int repl_timeout;
    time_t repl_transfer_lastio;
    time_t repl_down_since;
    // replicate slave
    char replid[CONFIG_REPL_RUNID_LEN+1];
    char replid2[CONFIG_REPL_RUNID_LEN+1];
    off_t second_replid_offset;

    int repl_diskless_sync;
    int repl_diskless_sync_delay;
    int repl_state;
    int repl_transfer_s;
    char *master_host;
    int master_port;
    char *master_auth;
    long long repl_send_timeout;
    long long repl_read_timeout;
    off_t master_initial_offset;
    char master_replid[CONFIG_REPL_RUNID_LEN+1];
    client *cache_master;
    client *master;
    int repl_transfer_tmp_fd;
    long long repl_transfer_size;
    char repl_transfer_tmp_file[255];
    size_t repl_transfer_nread;
    off_t repl_transfer_last_sync_off;
    int aof_repl_read_from_child;
    int aof_repl_write_to_parent;

    long long repl_backlog_time_limit; // replication backlog will release when no slaves and ttl.
    time_t repl_backlog_no_slaves_since;
    long repl_slave_send_timeout;
    struct redisCommand *expire_command;
    struct redisCommand *pexpire_command;
    struct redisCommand *multi_command;
}redisServer;

typedef struct pubsubPattern {
    robj *pattern;
    client *client;
}pubsubPattern;

typedef struct watchKey {
    redisDb *db;
    robj *key;
}watchKey;

typedef struct aof_rwblock {
    size_t free;
    size_t used;
    char buf[AOF_REWRITE_BLOCK_SIZE];
}aof_rwblock;

#define OBJ_SHARED_INTEGERS 10000
#define OBJ_BULK_LEN_SIZE 32
#define OBJ_SHARED_REFCOUNT INT_MAX
#define OBJ_SHARED_COMMAND_SIZE 10
struct redisSharedObject {
    robj *crlf, *ok, *syntaxerr, *nullbulk, *wrongtypeerr, *nullmultibulk, *emptymultibulk, *loadingerr,*pong,
    *integers[OBJ_SHARED_INTEGERS],
    *mbulkhdr[OBJ_BULK_LEN_SIZE],
    *commands[OBJ_SHARED_COMMAND_SIZE],
    *bulkhdr[OBJ_BULK_LEN_SIZE], *czero, *cone, *subscribe, *psubscribe, *queued, *execaborterr;
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
void updateCachedTime();

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

// object type equal.
int listValueEqual(void *key1, void *key2);

// robj string type, equal method.
int stringObjectEqual(robj *robj1, robj *robj2);

// create the golbal shared object
void createSharedObject(void);

// check key type matchs the giving param type, return 1 match, otherwise 0 return.
int checkType(robj *key, int type);

// get timeout
int getTimeoutFromObjectOrReply(client *c, robj* argv, int unit, long long *timeout, robj *reply);

//-------------- redis commands process prototype -----------
void populateCommandTable(void);

// lookup commands by redis object, should be the string type object.
struct redisCommand* lookupCommand(robj *o);

void selectCommand(client *c);

// getCommand get the value from kv, if key exists but not string type, client will receive wrong type error.
// as side effect, key will delete when go expire, client will receive nullbulk.
void getCommand(client *c);

// setCommand set the kv,
// set key value [EX EXPIRES] [PX milliseconds] [NX|XX]
void setCommand(client *c);

// expire the key, value is ttl seconds.
void expireCommand(client *c);

// expire the key, value is ttl milliseconds.
void pexpireCommand(client *c);

void expireGenericCommand(client *c, int unit);

// utils for redis commands prototype
// generic get commands
int getGenericCommand(client *c);

// generic set commands
int setGenericCommand(client *c, robj *key, robj *value, int flags, robj *expires, int unit, robj *ok_reply, robj *abort_reply);

// set multi key value
void msetCommand(client *c);

// get multi key value
void mgetCommand(client *c);

// set multi key value generic commands
int msetGenericCommand(client *c, int nx);

// client use which db.
void selectDb(client *c, int id);

// get the key ttl
void ttlCommand(client *c);

// get the key pttl
void pttlCommand(client *c);

// lrange subject [start, end]
void lrangeCommand(client *c);

// ltrim subject [start, end]
void ltrimCommand(client *c);

// push an element into head.
void lpushCommand(client *c);

// push an element into head.
void rpushCommand(client *c);

// pop an element from head.
void lpopCommand(client *c);

// pop an element from tail.
void rpopCommand(client *c);

// pop an element from head, when not exists, block the client with options.
void blpopCommand(client *c);

// pop and element from tail, when not exists, block the client with options.
void brpopCommand(client *c);

// subscribe the channel.
void subscribeCommand(client *c);

// subscribe the pattern with glob style input.
void psubscribeCommand(client *c);

// publish the message to the client who is interesting.
void publishCommand(client *c);

// for blpop, brpop.
void blockingGenericCommand(client *c, int where);

// start the transaction.
void multiCommand(client *c);

// watch the keys.
void watchCommand(client *c);

// cancel watch the key
void unWatchCommand(client *c);

// discard the transaction.
void discardCommand(client *c);

// exec the transaction.
void execCommand(client *c);

// queued the commands on multi context.
void queueMultiCommand(client *c);

// ping command
void pingCommand(client *c);

// ------------ replication commands ------------
void syncCommand(client *c);
void replConfCommand(client *c);
void slaveofCommand(client *c);

// block client for multi input keys.
void blockForKeys(client *c, robj **argv, int argc, long long timeout);

// signal list if there is blocking client waiting for data.
void signalListAsReady(redisDb *db, robj *key);

// handle blocked clients at least one key is ready.
void handleClientsOnBlockedList(void);

// unblock client on blocking set
void unblockClient(client *c);

// serve client on blocked list.
int serveClientOnBlockedList(client *c, robj *key, robj *value);

// subscribe channel.
int pubsubSubscribeChannel(client *c, robj *channel);

// subscribe pattern except glob like style input.
int pubsubSubscribePattern(client *c, robj *pattern);

void disconnectAllBlockedClients(void);

// get the clients pubsub count.
unsigned long clientSubscriptionCount(client *c);

// publish message to all subscribe clients.
int pubsubPublishMessage(robj *channel, robj *message);

// execute the commands.
void call(client *c, int flags);

// ----------- commands utils -------------

// delete the key from expires set if exists, but logically has been expired.
// return 1 if key remove from expires, 0 key not exists.
int expireIfNeed(redisDb *db, robj *key);

// lookup a key from select db, null received when key not exists or has been expired.
// as a side effect, if key exists but logical expired, will delete by using dbSyncDelete or dbAsyncDelete.
robj *lookupKeyRead(redisDb *db, robj *key);

// lookup a key from select db, null received when key not exists or has been expired.
// as a side effect, if key exists but logical expired, will delete by using dbSyncDelete or dbAsyncDelete.
robj *lookupKeyWrite(redisDb *db, robj *key);

// lookup a key from select db, null received when key not exists or has been expired.
// as a side effect, if key exists but logical expired, will delete by using dbSyncDelete or dbAsyncDelete.
// if key not exists or expired, reply will send to client if is not null.
robj *lookupKeyReadOrReply(client *c, robj *key, robj *reply);

// redis commands prototype
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
void removeExpire(redisDb *db, robj *key);

// sync delete a key from db.
int dbSyncDelete(redisDb *db, robj *key);

// get the key expire milliseconds
long long getExpire(redisDb *db, robj *key);

// signal key as modified. useful for the client which interested the keys.
void signalKeyAsModified(redisDb *db, robj *key);

// clear the dbid.
int emptyDb(int dbid);

//------------- list prototype --------------

// push commands, lpush or rpush, where -> LIST_HEAD or LIST_TAIL.
void pushGenericCommand(client *c, int where);

// pop commands, lpop or rpop, where -> LIST_HEAD or LIST_TAIL
void popGenericCommand(client *c, int where);

// push.
void listTypePush(robj *subject, robj *value, int where);

// pop.
robj* listTypePop(robj *subject, int where);

// pop saver.
void* listPopSaver(void *data, unsigned int size);

// list len.
unsigned int listTypeLen(robj *lobj);

//-------------multi prototype---------

// watch a key.
void watchForKey(client *c, robj *key);

// unwatch all keys.
void unWatchAllKeys(client *c);

// flag transaction as dirty, so will exec failed.
void flagTransactionAsDirty(client *c);

// touch a key which been watched.
void touchWatchedKey(redisDb *db, robj *key);

// discard multi transaction.
void discardTransaction(client *c);

//-------------syncio-------------------
size_t syncWrite(int fd, char *ptr, size_t size, long long timeout);
size_t syncRead(int fd, char *ptr, size_t size, long long timeout);
size_t syncReadLine(int fd, char *ptr, size_t size, long long timeout);
//-------------networking----------------
void acceptTcpHandler(struct eventLoop *el, int fd, int mask, void *clientData);
void acceptCommandHandler(int cfd, char *ip, int port);

int processMultiBulkBuffer(client *client);
int processInlineBuffer(client *client);

int clientHasPendingWrites(client *c);
void sendClientData (struct eventLoop *el, int fd, int mask, void *clientData);
int writeToClient(client *client, int handler_installed);
void handleClientsPendingWrite(void);
void processEventsWhileBlocked(void);
void readQueryFromClient(eventLoop *el, int fd, int mask, void *clientData);
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
void replyUnBlockClientTimeout(client *c);
void copyClientOutputBuffer(client *src, client *dst);
// ------------redis client--------------

// reset the client so it can process commands again.
void resetClient(client *c);
client* createClient(int fd);
void linkClient(client *c);
void unlinkClient(client *c);
void freeClientArgv(client *c);
void freeClient(client *c);
void freeClientAsync(client *c);
void freeClientInFreeQueueAsync(void);
void initClientMultiState(client *c);
void freeClientMultiState(client *c);
//-------------cron job-----------------

void clientCron(void);
void clientHandleCronTimeout(client *c, mstime_t nowms);
void replicationCron(void);
long long serverCron(struct eventLoop *el, int id, void *clientData);


// ------------ replicate master ---------------
int startBgAofSaveForReplication(int mincapa);
int startBgAofSaveForSlaveSockets();
void replicationFeedSlaves(int dbid, robj **argv, int argc);
int addReplyReplicationBacklog(client *c, long long psync_offset);
void feedReplicationBacklog(void *ptr, size_t len);
void feedReplicationBacklogObject(robj *obj);
int masterTryPartialResynchronization(client *c);
void changeReplicationId(void);
void clearReplicationId2(void);
void shiftReplicationId(void);
// ------------ replicate slave -----------------
int connectWithMaster(void);
void syncWithMaster(struct eventLoop *el, int fd, int mask, void *clientData);
void readSyncBulkPayload(struct eventLoop *el, int fd, int mask, void *clientData);

void replicationDiscardCacheMaster(void);
void replicationResurrectCacheMaster(int fd);
void replicationUnsetMaster(void);
void disconnectSlaves(void);
void replicationCreateMaterClient(int fd);
int replicationIsInHandshake(void);
void undoConnectWithMaster(void);
void cancelReplicationHandShake(void);
void replicationAbortSyncTransfer(void);
void replicationSetMaster(char *host, long port);
void replicationCacheMaster(void);
void replicationHandleMasterDisconnection(void);
int slaveTryPartialResynchronization(int fd, int read_reply);
void replicationSendAck();
// ------------process commands -------------
int processCommand(client *c);

// ------------propagate commands --------------
void propagate(struct redisCommand *cmd, int dbid, int argc, robj **argv, int flag);
void propagateCommand(client *c, int flags);
void execCommandPropagateMulti(client *c);
// --------------- aof -----------------
ssize_t aofWrite(sds buf, size_t len);
sds catAppendOnlyFileExpireCommand(sds buf, struct redisCommand *cmd, robj *key, robj *expire);
sds catAppendOnlyFileGenericCommand(sds buf, int argc, robj **argv);
void feedAppendOnlyFile(struct redisCommand *cmd, int dbid, int argc, robj **argv);
void aofRewriteBufferAppend(sds buf);
void pipeFromChildReadable(struct eventLoop *el, int fd, int mask, void *clientData);
void aofRewriteBufferPipeWritable(struct eventLoop *el, int fd, int mask, void *clientData);
size_t aofRewriteBufferWrite(int fd);
void aofRewriteBufferReset();
void aofRemoveTempFile();
void flushAppendOnlyFile(int force);
client *createFakeClient(void);
void freeFakeClient(client *c);
int loadAppendOnlyFile(char *filename);
void aofUpdateCurrentSize(void);
int rewriteAppendOnlyFileBackground(void);
int rewriteAppendOnlyFileChild(char *name);
int rewriteAppendOnlyFile(FILE *fp);
int sendParentStopWriteAppendDiff(void);
size_t readDiffFromParent(void);
void aofDoneHandler(int bysignal, int code);
void aofRewriteDoneHandler(int bysignal, int code);
void aofDoneHandlerSlavesSocket(int bysignal, int code);
void aofDoneHandlerSlavesDisk(int bysignal, int code);
void exitFromChild(int code);
int aofCreatePipes(void);
void aofClosePipes(void);
void startLoading(FILE *fp);
void loadingProgress(off_t pos);
void killAppendOnlyChild(int force);
void stopAppendOnly();
int startAppendOnly();
void restartAOF();
int aofSaveToSlavesWithEOFMark(int *fds, int *states, int numfds);
// ---------- free method -----------------
void listFreePubsubPatterns(void *ptr);
void listFreeObject(void *ptr);
// ----------- dup method -----------------
void* listDupString(void *ptr);
#endif //REDIS_SERVER_H
