//
// Created by kieren jiang on 2023/2/20.
//

#include "server.h"

// dict key type sds, ignore case-sensitive.
uint64_t dictSdsCaseHash(void *key) {
    return crc64_nocase((const unsigned char *)(key), sdslen(key));
}

// dict key type sds, case-sensitive.
uint64_t dictSdsHash(void *key) {
    return crc64((const unsigned char *)(key), sdslen((char *)(key)));
}

// dict key type sds
int dictSdsCompare(const void *ptr1, const void *ptr2) {
    size_t s1len = sdslen((sds)ptr1);
    size_t s2len = sdslen((sds)ptr2);
    if (s1len != s2len) return 0;
    return memcmp(ptr1, ptr2, s1len > s2len ? s2len : s1len) == 0;
}

// dict key type sds, case-insensitive
int dictSdsCaseCompare(const void *ptr1, const void *ptr2) {
    return strcasecmp((const char *)(ptr1), (const char *)(ptr2)) == 0;
}

void dictSdsDestructor(void *ptr) {
    sdsfree((sds)ptr);
}

void dictObjDestructor(void *ptr) {
    decrRefCount(ptr);
}

uint64_t dictEncObjectHash(void *ptr) {

    robj *obj = ptr;

    if (sdsEncodedObject(obj)) {
        return dictSdsHash(obj->ptr);
    } else {
        obj = getDecodedObject(obj);
        uint64_t h = dictSdsHash(obj->ptr);
        decrRefCount(obj);
        return h;
    }
}

int dictEncObjectCompare(const void *key1, const void *key2) {

    robj *obj1, *obj2;
    obj1 = (robj*)key1;
    obj2 = (robj*)key2;

    if (obj1->encoding == REDIS_ENCODING_INT && obj2->encoding == REDIS_ENCODING_INT) {
        long val1, val2;
        val1 = (long)obj1->ptr;
        val2 = (long)obj2->ptr;
        return val1 == val2;
    }

    obj1 = getDecodedObject(obj1);
    obj2 = getDecodedObject(obj2);
    int cmp = dictSdsCompare(obj1->ptr, obj2->ptr);
    decrRefCount(obj1);
    decrRefCount(obj2);
    return cmp;
}

void dictListDestructor(void *ptr) {
    listRelease(ptr);
}

// global vars
struct redisServer server;
struct redisSharedObject shared;

struct redisCommand redisCommandTable[] = {
    {"select", selectCommand, 2},
    {"get", getCommand, 2},
    {"set", setCommand, -3},
    {"expire", expireCommand, 3},
    {"pexpire", pexpireCommand, 3},
    {"ttl", ttlCommand, 2},
    {"pttl", pttlCommand, 2},
    {"mset", msetCommand, -3},
    {"mget", mgetCommand, -2},
    {"lpush", lpushCommand, -3},
    {"rpush", rpushCommand, -3},
    {"lpop", lpopCommand, 2},
    {"rpop", rpopCommand, 2},
    {"blpop", blpopCommand, -3},
    {"brpop", brpopCommand, -3},
    {"lrange", lrangeCommand, 4},
    {"ltrim", ltrimCommand, 4},
    {"subscribe", subscribeCommand, -2},
    {"psubscribe", psubscribeCommand, -2},
    {"publish", publishCommand, 3},
    {"multi", multiCommand, 1},
    {"watch", watchCommand, -2},
    {"unwatch", unWatchCommand, 1},
    {"exec", execCommand, 1},
    {"discard", discardCommand, 1},

};

// dict type for commands table
dictType commandTableDictType = {
        dictSdsCaseHash,
        dictSdsCaseCompare,
        NULL,
        NULL,
        dictSdsDestructor,
        NULL,
};


dictType dbDictType = {
        dictSdsHash,
        dictSdsCompare,
        NULL,
        NULL,
        dictSdsDestructor,
        dictObjDestructor,
};

dictType keyptrDictType = {
        dictSdsHash,
        dictSdsCompare,
        NULL,
        NULL,
        NULL,
        NULL,
};

dictType objectKeyValueListDictType = {
        dictEncObjectHash,
        dictEncObjectCompare,
        NULL,
        NULL,
        dictObjDestructor,
        dictListDestructor,
};

dictType objectKeyValuePtrDictType = {
        dictEncObjectHash,
        dictEncObjectCompare,
        NULL,
        NULL,
        dictObjDestructor,
        NULL,
};

mstime_t mstime() {
    struct timeval t;
    gettimeofday(&t, NULL);
    mstime_t milliseconds= 0;
    milliseconds = t.tv_sec * 1000 + t.tv_usec / 1000;
    return milliseconds;
}


void createSharedObject(void) {

    shared.crlf = createObject(REDIS_OBJECT_STRING, sdsnew("\r\n"));
    shared.ok = createObject(REDIS_OBJECT_STRING, sdsnew("+ok\r\n"));
    shared.syntaxerr = createObject(REDIS_OBJECT_STRING, sdsnew("-ERR syntax err\r\n"));
    shared.wrongtypeerr = createObject(REDIS_OBJECT_STRING, sdsnew("-ERR wrong type against\r\n"));
    shared.nullbulk = createObject(REDIS_OBJECT_STRING, sdsnew("$-1\r\n"));
    shared.nullmultibulk = createObject(REDIS_OBJECT_STRING, sdsnew("*-1\r\n"));
    shared.emptymultibulk = createObject(REDIS_OBJECT_STRING, sdsnew("*0\r\n"));
    shared.loadingerr = createObject(REDIS_OBJECT_STRING, sdsnew("-Err loaded\r\n"));
    shared.czero = createObject(REDIS_OBJECT_STRING, sdsnew(":0\r\n"));
    shared.cone = createObject(REDIS_OBJECT_STRING, sdsnew(":1\r\n"));
    shared.subscribe = createObject(REDIS_OBJECT_STRING, sdsnew("$9\r\nsubscribe\r\n"));
    shared.psubscribe = createObject(REDIS_OBJECT_STRING, sdsnew("$10\r\npsubscribe\r\n"));
    shared.queued = createObject(REDIS_OBJECT_STRING, sdsnew("$6\r\nqueued\r\n"));
    shared.execaborterr = createObject(REDIS_OBJECT_STRING, sdsnew("-EXECABORT Transaction discarded because of previous errors.\r\n"));
    for (long j=0; j<OBJ_SHARED_INTEGERS; j++) {
        shared.integers[j] = createObject(REDIS_OBJECT_STRING, (void*)(j));
        shared.integers[j]->encoding = REDIS_ENCODING_INT;
        makeObjectShared(shared.integers[j]);
    }
    for (int j=0; j<OBJ_BULK_LEN_SIZE; j++) {
        sds s = sdscatfmt(sdsempty(), "$%i\r\n", j);
        shared.bulkhdr[j] = createObject(REDIS_OBJECT_STRING, s);
        makeObjectShared(shared.bulkhdr[j]);
    }

    for (int j=0; j<OBJ_BULK_LEN_SIZE; j++) {
        sds s = sdscatfmt(sdsempty(), "*%i\r\n", j);
        shared.mbulkhdr[j] = createObject(REDIS_OBJECT_STRING, s);
        makeObjectShared(shared.mbulkhdr[j]);
    }

    for (int j=0; j<OBJ_SHARED_COMMAND_SIZE; j++) {
        sds s = sdscatprintf(sdsempty(), "*2\r\n$6\r\nSELECT\r\n$1\r\n%d", j);
        shared.commands[j] = createObject(REDIS_OBJECT_STRING, s);
        makeObjectShared(shared.commands[j]);
    }

    makeObjectShared(shared.crlf);
    makeObjectShared(shared.ok);
    makeObjectShared(shared.syntaxerr);
    makeObjectShared(shared.wrongtypeerr);
    makeObjectShared(shared.nullbulk);
    makeObjectShared(shared.nullmultibulk);
    makeObjectShared(shared.emptymultibulk);
    makeObjectShared(shared.loadingerr);
    makeObjectShared(shared.cone);
    makeObjectShared(shared.czero);
    makeObjectShared(shared.subscribe);
    makeObjectShared(shared.psubscribe);
    makeObjectShared(shared.queued);
    makeObjectShared(shared.execaborterr);
}


void populateCommandTable(void) {
    int commandNums = sizeof(redisCommandTable) / sizeof(struct redisCommand);
    for (int j=0; j<commandNums; j++) {
        struct redisCommand *c = redisCommandTable+j;
        dictAdd(server.commands, sdsnew(c->name), c);
    }
}

struct redisCommand* lookupCommand(robj *o) {
    return dictFetchValue(server.commands, o->ptr);
}

void exitFromChild(int code) {
    _exit(code);
}

int listenPort(int backlog) {

    int unsupported = 0;

    // bind and listen inet_v6
    server.ipfd[server.ipfd_count] = anetTcp6Server(server.neterr, server.port, backlog);
    if (server.ipfd[server.ipfd_count] != ANET_ERR) {
        anetNonBlock(server.ipfd[server.ipfd_count]);
        fprintf(stdout, "server listen, fd=%d\n", server.ipfd[server.ipfd_count]);
        server.ipfd_count++;
    } else if (errno == EAFNOSUPPORT) {
        unsupported++;
    }

    // bind and listen inet_v4
    server.ipfd[server.ipfd_count] = anetTcpServer(server.neterr, server.port, backlog);
    if (server.ipfd[server.ipfd_count] != ANET_ERR) {
        anetNonBlock(server.ipfd[server.ipfd_count]);
        fprintf(stdout, "server listen, fd=%d\r\n", server.ipfd[server.ipfd_count]);
        server.ipfd_count++;
    } else if (errno == EAFNOSUPPORT) {
        unsupported++;
    }

    if (server.ipfd_count == 0) {
        return C_ERR; // non ip address binded
    }

    return C_OK;
}

void beforeSleep (struct eventLoop *el) {

    flushAppendOnlyFile(0);

    handleClientsPendingWrite();

}

long long serverCron(struct eventLoop *el, int id, void *clientData) {

    clientCron();

    updateCachedTime();

    freeClientInFreeQueueAsync();

    int statloc;
    pid_t pid;
    if ((pid = wait4(-1, &statloc, WNOHANG, NULL)) > 0) {

        int exitcode = 0;
        int bysignal = 0;

        if (WIFEXITED(statloc)) {
            exitcode = WEXITSTATUS(statloc);
        }

        if (WIFSIGNALED(statloc)) {
            bysignal = WTERMSIG(statloc);
        }

        if (pid == server.aof_child_pid) {
            aofDoneHandler(bysignal, exitcode);
        }

        dictEnableResize();
    }

    // todo with 1000ms period
    slaveCron();


    return SERVER_CRON_PERIOD_MS;
}

void replyUnBlockClientTimeout(client *c) {
    addReply(c, shared.nullmultibulk);
}

void clientHandleCronTimeout(client *c, mstime_t nowms) {
    if (c->flag & CLIENT_BLOCKED) {
        if (c->bpop.timeout != 0 && (c->bpop.timeout < nowms)) {
            replyUnBlockClientTimeout(c);
            unblockClient(c);
        }
    }
}

#define CLIENT_CRON_MIN_ITERATION 5
void clientCron(void) {

    int clientsnum = listLength(server.client_list);

    int iterations = clientsnum > CLIENT_CRON_MIN_ITERATION ? CLIENT_CRON_MIN_ITERATION : clientsnum;

    mstime_t now = mstime();

    while (listLength(server.client_list) && iterations--) {

        listNode *ln = listFirst(server.client_list);
        client *c = listNodeValue(ln);
        listDelNode(server.client_list, ln);
        clientHandleCronTimeout(c, now);
    }

}

void slaveCron(void) {


    //-------------- replicate ----------------

    if (server.master && server.master_host && server.repl_state == REPL_STATE_CONNECT) {
        connectWithMaster();
    }


    //-------------- master -------------------
    // each 10s ping our slaves




}

void updateCachedTime() {
    server.unix_time = time(NULL);
}

void loadDataFromDisk() {
    if (server.aof_state == AOF_ON) {
        loadAppendOnlyFile(server.aof_filename);
    }
}


void initServer(void) {

    robj *expire_command_key, *pexpire_command_key, *multi_command_key;

    createSharedObject();

    server.nextid = 0;

    // create commands
    server.commands = dictCreate(&commandTableDictType);
    populateCommandTable();

    // create dbs
    server.dbnum = REDIS_DEFAULT_DB_NUM;
    server.dbs = zmalloc(sizeof(redisDb) * server.dbnum);
    for (int j=0; j<server.dbnum; j++) {
        server.dbs[j].id = j;
        server.dbs[j].dict = dictCreate(&dbDictType);
        server.dbs[j].expires = dictCreate(&keyptrDictType);
        server.dbs[j].blocking_keys = dictCreate(&objectKeyValueListDictType);
        server.dbs[j].ready_keys = dictCreate(&objectKeyValuePtrDictType);
        server.dbs[j].watch_keys = dictCreate(&objectKeyValueListDictType);
    }

    expire_command_key = createStringObject("EXPIRE", 6);
    pexpire_command_key = createStringObject("PEXPIRE", 7);
    multi_command_key = createStringObject("MULTI", 5);
    server.list_fill_factor = DEFAULT_LIST_FILL_FACTOR;
    server.backlog = DEFAULT_BACKLOG;
    server.port = DEFAULT_BIND_PORT;
    updateCachedTime();
    server.el = elCreateEventLoop(1024);
    server.client_pending_writes = listCreate();
    server.client_list = listCreate();
    server.client_close_list = listCreate();
    server.ready_keys = listCreate();
    server.pubsub_channels = dictCreate(&objectKeyValueListDictType);
    server.pubsub_patterns = listCreate();
    server.aof_seldb = -1;
    server.aof_buf = sdsempty();
    server.aof_filename = DEFAULT_AOF_FILENAME;
    server.dirty = 0;
    server.expire_command = lookupCommand(expire_command_key);
    server.pexpire_command = lookupCommand(pexpire_command_key);
    server.multi_command = lookupCommand(multi_command_key);
    server.aof_fsync = AOF_FSYNC_EVERYSEC;
    server.aof_postponed_start = 0;
    server.aof_update_size = 0;
    server.aof_last_fsync = -1;
    server.loading = 0;
    server.aof_state = AOF_ON;
    server.aof_loaded_bytes = 0;
    server.aof_loading_total_bytes = 0;
    server.aof_loaded_truncated = 1;
    server.aof_child_pid = -1;
    server.aof_rw_block_list = listCreate();
    listSetFreeMethod(server.aof_rw_block_list, zfree);
    server.aof_pipe_read_data_from_parent = -1;
    server.aof_pipe_write_data_to_child = -1;
    server.aof_pipe_read_ack_from_child = -1;
    server.aof_pipe_write_ack_to_parent = -1;
    server.aof_pipe_read_ack_from_parent = -1;
    server.aof_pipe_write_ack_to_child = -1;
    server.aof_stop_sending_diff = -1;
    server.repl_diskless_sync = CONFIG_REPL_DISKLESS_SYNC;
    server.repl_backlog_size = CONFIG_REPL_BACKLOG_SIZE;
    server.repl_backlog_idx = 0l;
    server.repl_backlog_histlen = 0l;
    server.repl_backlog_off = 0l;
    server.master_repl_offset = 0l;
    server.aof_type = AOF_TYPE_NONE;
    server.aof_repl_read_from_child = -1;
    server.aof_repl_write_to_parent = -1;
    server.repl_seldbid = -1;
    server.slaves = listCreate();

    listSetFreeMethod(server.slaves, zfree);
    listSetFreeMethod(server.client_list, zfree); // free the client which alloc from heap
    listSetMatchMethod(server.pubsub_patterns, listValueEqual);
    listSetFreeMethod(server.pubsub_patterns, listFreePubsubPatterns);
    if (listenPort(server.backlog) == C_ERR) {
        exit(1);
    }

    for (int i=0; i<server.ipfd_count; i++) {
        int fd = server.ipfd[i];
        if (elCreateFileEvent(server.el, fd, EL_READABLE, acceptTcpHandler, NULL) == EL_ERR) {
            exit(1);
        }
    }

    // open aof
    server.aof_fd = open(server.aof_filename, O_CREAT | O_APPEND | O_WRONLY, 0644);
    if (server.aof_fd == -1) {
        exit(-1);
    }

    loadDataFromDisk();
    bioInit();
    elSetBeforeSleepProc(server.el, beforeSleep);
    elCreateTimerEvent(server.el, SERVER_CRON_PERIOD_MS, serverCron, NULL, NULL);

    decrRefCount(expire_command_key);
    decrRefCount(pexpire_command_key);
    decrRefCount(multi_command_key);
}

void selectDb(client *c, int id) {
    if (id < 0 || id >= server.dbnum) {
        // todo server panic
    }
    c->db = &server.dbs[id];
}

void call(client *c, int flags) {

    unsigned long long dirty = server.dirty;

    c->cmd->proc(c);

    if (dirty != server.dirty) {
        propagateCommand(c, flags);
    }

}

int processCommand(client *c) {

    if (strcasecmp(c->argv[0]->ptr, "quit") == 0) {
        sds ok = shared.ok->ptr;
        addReplyString(c, ok, sdslen(ok));
        c->flag |= CLIENT_CLOSE_AFTER_REPLY;
        return C_ERR;
    }

    c->cmd=lookupCommand(c->argv[0]);

    if (c->cmd == NULL) {
        flagTransactionAsDirty(c);
        addReplyError(c, "unknown commands");
        return C_OK;
    }


    if ((c->cmd->arity > 0 && c->argc != c->cmd->arity)
        || (c->cmd->arity <0 && c->argc < -c->cmd->arity)) {
        flagTransactionAsDirty(c);
        addReplyError(c, "invalid argument");
        return C_OK;
    }

    if (server.loading) {
        addReply(c, shared.loadingerr);
        return C_OK;
    }

    if ((c->flag & CLIENT_MULTI) &&
            c->cmd->proc != execCommand &&
            c->cmd->proc != multiCommand &&
            c->cmd->proc != watchCommand &&
            c->cmd->proc != discardCommand) {

        queueMultiCommand(c);

    } else {

        call(c, PROPAGATE_CMD_FULL);
    }

    if (listLength(server.ready_keys))
        handleClientsOnBlockedList();

    return C_OK;
}

void listFreePubsubPatterns(void *ptr) {
    // todo make sure ptr is pubsubPattern
    pubsubPattern *pat = ptr;
    decrRefCount(pat->pattern);
    zfree(pat);
}

void listFreeObject(void *ptr) {
    // todo make sure ptr is robj
    decrRefCount(ptr);
}

void* listDupString(void *ptr) {
    char *s = ptr;
    size_t len = strlen(s);
    char *copy = zmalloc(sizeof(char) * (len+1));
    memcpy(copy, s, len);
    s[len] = '\0';
    return copy;
}

void propagate(struct redisCommand *cmd, int dbid, int argc, robj **argv, int flags) {
    if (flags & PROPAGATE_CMD_AOF && server.aof_state == AOF_ON) {
        feedAppendOnlyFile(cmd, dbid, argc, argv);
    }
}

void propagateCommand(client *c, int flags) {
    propagate(c->cmd, c->db->id, c->argc, c->argv, flags);
}

int main(int argc, char **argv) {
    // testZiplist();
    // quicklistTest();
//    char _buf[1024];
//    memset(_buf, 'h', sizeof(_buf)-1);
//    _buf[1023] = '\0';
//    sds buf = sdsempty();
//    buf = sdscatfmt(buf, "%i\r\n", 10);
//    buf = sdscatsds(buf, sdsnew(_buf));
//    sdsfree(buf);

    printf("server start...., pid=%d\n", getpid());
    initServer();
    elMain(server.el);
    return 0;
}



