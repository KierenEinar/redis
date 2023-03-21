//
// Created by kieren jiang on 2023/2/20.
//
#include "server.h"

int getExpire(db *db, robj *key, mstime_t *expireMilliSeconds) {
    dictEntry *de = dictFind(db->expires, key->ptr);
    if (!de) return -1;
    mstime_t now = ms_now();
    mstime_t val = de->v.s_64 - now;
    if (expireMilliSeconds) *expireMilliSeconds = val;
    return val <= 0 ? 1 : 0;
}

int dbSyncDelete(db *db, robj *key) {
    dictDelete(db->expires, key->ptr);
    if (dictDelete(db->dict, key->ptr)) {
        return 1;
    }
    return 0;
}

robj* lookupKeyWrite(db *db, robj *key) {
    expireIfNeeded(db, key);
    return lookupKey(db, key, LOOKUP_TOUCH);
}

int expireIfNeeded(db *db, robj *key) {
    mstime_t expire = 0;

    int expired = getExpire(db, key, &expire);
    if (expired == 0 || expired == -1) {
        return 0;
    }
    return dbSyncDelete(db, key);
}

robj* lookupKey(db *db, robj *key, int flags) {

    dictEntry *de = dictFind(db->dict, key->ptr);
    if (!de) return NULL;

    robj *o = (robj *)de->v.value;

    if (!(flags & LOOKUP_NOTOUCH)) {
        // todo update lru bit
    }

    return o;
}

void setKey(db *db, robj *key, robj *val){
    dictEntry *de = dictFind(db->dict, key->ptr);
    if (!de) {
        dbAddRow(db->dict, key, val);
    } else {
        dbOverwrite(db->dict, key, val);
    }
}