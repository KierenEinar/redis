//
// Created by kieren jiang on 2023/7/3.
//

#include "server.h"

void dbAdd(client *c, robj *key, robj *value) {
    dictAdd(c->db->dict, sdsdup(key->ptr), value);
}

void dbReplace(client *c, robj *key, robj *value) {
    dictReplace(c->db->dict, key->ptr, value);
}

void removeExpire(client *c, robj *key) {
    if (dictSize(c->db->expires) == 0) return;
    dictDelete(c->db->expires, key->ptr);
}

void setKey(client *c, robj *key, robj *value) {

    if (!lookupKeyWrite(c, key)) {
        dbAdd(c, key, value);
    } else {
        dbReplace(c, key, value);
    }
    incrRefCount(value);
    removeExpire(c, key);
}

void setExpire(client *c, robj *key, long long expire) {
    dictEntry *entry = dictAddRow(c->db->expires, key->ptr, NULL);
    mstime_t now = mstime();
    entry->value.s64 = now + expire;
}