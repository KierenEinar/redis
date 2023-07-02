//
// Created by kieren jiang on 2023/6/30.
//

#include "server.h"



int expireIfNeed(client *c, const robj *key) {

    int64_t expired;
    if (dictSize(c->db->expires) == 0) return 0;
    int retval = dictGetSignedInteger(c->db->expires, key->ptr, &expired);
    if (retval == 0) return 0;
    mstime_t ms = mstime();
    if (expired >= ms) return 0;
    return 1;
}

robj *lookupKey(client *c, const robj *key) {
    int64_t expired;
    if (dictSize(c->db->dict) == 0) return NULL;

    robj *value = dictFetchValue(c->db->dict, key);
    if (value == NULL) return NULL;

    int retval = dictGetSignedInteger(c->db->expires, key->ptr, &expired);
    if (retval == 0) return value;

    dictDelete(c->db->expires, key->ptr);
    dictDelete(c->db->dict, key->ptr);
    return NULL;
}

robj *lookupKeyRead(client *c, const robj *key) {
    expireIfNeed(c, key);
    return lookupKey(c, key);
}

robj *lookupKeyWrite(client *c, const robj *key) {
    expireIfNeed(c, key);
    return lookupKey(c, key);
}

robj *lookupKeyReadOrReply(client *c, robj *key, robj *reply) {
    expireIfNeed(c, key);
    robj *val = lookupKey(c, key);
    if (!val) {
        if (reply) {
            // todo send reply to client
        } else {
            addReplyString(c, shared.nullbulk->ptr, sdslen(shared.nullbulk->ptr));
        }
    }
    return val;
}

int getGenericCommand(client *c) {
    robj *value = lookupKeyReadOrReply(c, c->argv[1],NULL);
    if (value == NULL) return C_ERR;
    return C_OK;
}


void getCommand(client *c) {
    getGenericCommand(c);
}
