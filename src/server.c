//
// Created by kieren jiang on 2023/2/20.
//

#include "server.h"
#include "varint.h"

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

// global vars
struct redisServer server;
struct redisSharedObject shared;

struct redisCommand redisCommandTable[] = {
    {"get", getCommand, 2},
    {"set", setCommand, -3},
};

// dict type for command table
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


mstime_t mstime() {
    struct timeval t;
    gettimeofday(&t, NULL);
    mstime_t milliseconds= 0;
    milliseconds = t.tv_sec * 1000 + t.tv_usec / 1000;
    return milliseconds;
}


void createSharedObject(void) {

    shared.crlf = createStringObject("\r\n", 2);
    shared.ok = createStringObject("+ok\r\n", 5);
    shared.syntaxerr = createStringObject("-ERR syntax err\r\n", 17);
    shared.wrongtypeerr = createStringObject("-ERR wrong type against\r\n", 25);
    shared.nullbulk = createStringObject("$-1\r\n", 5);
    for (long j=0; j<OBJ_SHARED_INTEGERS; j++) {
        shared.integers[j] = createObject(REDIS_OBJECT_STRING, (void*)(j));
        shared.integers[j]->encoding = REDIS_ENCODING_INT;
        makeObjectShared(shared.integers[j]);
    }
    for (int j=0; j<OBJ_BULK_LEN_SIZE; j++) {
        sds s = sdscatfmt(sdsempty(), "$%i\r\n", j);
        shared.bulkhdr[j] = createObject(REDIS_OBJECT_STRING, s);
        shared.bulkhdr[j]->encoding = REDIS_ENCODING_RAW;
        makeObjectShared(shared.bulkhdr[j]);
    }

    makeObjectShared(shared.crlf);
    makeObjectShared(shared.ok);
    makeObjectShared(shared.syntaxerr);
    makeObjectShared(shared.nullbulk);
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
        fprintf(stdout, "server listen, fd=%d\r\n", server.ipfd[server.ipfd_count]);
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
    handleClientsPendingWrite();
}

long long serverCron(struct eventLoop *el, int id, void *clientData) {
    freeClientInFreeQueueAsync();

    return SERVER_CRON_PERIOD_MS;
}


void initServer(void) {

    createSharedObject();

    // create commands
    server.commands = dictCreate(&commandTableDictType);
    populateCommandTable();

    // create dbs
    server.dbnum = REDIS_DEFAULT_DB_NUM;
    server.dbs = zmalloc(sizeof(redisDb) * server.dbnum);
    for (int j=0; j<server.dbnum; j++) {
        server.dbs[j].dict = dictCreate(&dbDictType);
        server.dbs[j].expires = dictCreate(&keyptrDictType);
    }


    server.backlog = DEFAULT_BACKLOG;
    server.port = DEFAULT_BIND_PORT;
    server.unix_time = time(NULL);
    server.el = elCreateEventLoop(1024);
    server.client_pending_writes = listCreate();
    server.client_list = listCreate();
    server.client_close_list = listCreate();
    listSetFreeMethod(server.client_list, zfree); // free the client which alloc from heap

    if (listenPort(server.backlog) == C_ERR) {
        exit(1);
    }

    for (int i=0; i<server.ipfd_count; i++) {
        int fd = server.ipfd[i];
        if (elCreateFileEvent(server.el, fd, EL_READABLE, acceptTcpHandler, NULL) == EL_ERR) {
            exit(1);
        }
    }

    elSetBeforeSleepProc(server.el, beforeSleep);

    elCreateTimerEvent(server.el, SERVER_CRON_PERIOD_MS, serverCron, NULL, NULL);

}

void selectDb(client *c, int id) {
    if (id < 0 || id >= server.dbnum) {
        // todo server panic
    }
    c->db = &server.dbs[id];
}

void call(client *c) {

    c->cmd->proc(c);
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
        addReplyError(c, "unknown command");
        return C_OK;
    }


    if ( (c->cmd->arity > 0 && c->argc != c->cmd->arity)
        || (c->cmd->arity <0 && c->argc < -c->cmd->arity)) {
        addReplyError(c, "invalid argument");
        return C_OK;
    }

    call(c);

    return C_OK;
}


int main(int argc, char **argv) {
    printf("server start...., pid=%d\r\n", getpid());
//    printf("crc64 = %llx\r\n", crc64((const unsigned char*)"Hello world", strlen("hello world")));
//    printf("crc64_nocase = %llx\r\n", crc64_nocase((const unsigned char*)"Hello world", strlen("hello world")));
    initServer();
    elMain(server.el);
    return 0;
}



