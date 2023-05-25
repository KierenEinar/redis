//
// Created by kieren jiang on 2023/2/20.
//
#include "server.h"

long long getExpire(db *db, robj *key) {
    dictEntry *de;
    if ((de = dictFind(db->expires, key->ptr)) == NULL) return -1;
    // todo  assert db->dict containse key
    return dictFetchSignedInteger(de);
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