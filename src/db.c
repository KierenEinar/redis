//
// Created by kieren jiang on 2023/7/3.
//

#include "server.h"

void dbAdd(client *c, robj *key, robj *value) {
    dictAdd(c->db->dict, sdsdup(key->ptr), value);
    if (value->type == REDIS_OBJECT_LIST) signalListAsReady(c->db, key);
}

void dbReplace(client *c, robj *key, robj *value) {
    dictReplace(c->db->dict, key->ptr, value);
}

void removeExpire(redisDb *db, robj *key) {
    if (dictSize(db->expires) == 0) return;
    dictDelete(db->expires, key->ptr);
}

int dbSyncDelete(redisDb *db, robj *key) {
    if (dictFind(db->expires, key)) dictDelete(db->expires, key);
    if (dictFind(db->dict, key)) {
        dictDelete(db->dict, key);
        return 1;
    }
    return 0;
}

void setKey(client *c, robj *key, robj *value) {

    if (!lookupKeyWrite(c->db, key)) {
        dbAdd(c, key, value);
    } else {
        dbReplace(c, key, value);
    }
    incrRefCount(value);
    removeExpire(c->db, key);
    signalKeyAsModified(c->db, key);
}

void setExpire(client *c, robj *key, long long expire) {
    fprintf(stdout, "dict size=%ld\n", dictSize(c->db->expires));
    dictEntry *de, *expireRow;
    de = dictFind(c->db->dict, key->ptr);
    // todo assert de not null
    expireRow = dictAddOrFind(c->db->expires, de->key);
    mstime_t now = mstime();
    expireRow->value.s64 = now + expire;
}

long long getExpire(redisDb *db, robj *key) {

    dictEntry *de;

    if (dictSize(db->expires) == 0 || (de = dictFind(db->expires, key->ptr)) == NULL) {
        return -1;
    }

    // todo assert de not null
    return dictGetSignedInteger(de);
}

void signalKeyAsModified(redisDb *db, robj *key) {
    touchWatchedKey(db, key);
}

// todo implement.
int emptyDb(int dbid) {
    return 0;
}