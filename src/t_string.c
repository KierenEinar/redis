//
// Created by kieren jiang on 2023/6/30.
//

#include "server.h"

// -------- define set flags -----------
#define SET_OBJECT_NO_FLAG 0
#define SET_OBJECT_NX 1 << 0
#define SET_OBJECT_XX 1 << 1
#define SET_OBJECT_EX 1 << 2
#define SET_OBJECT_PX 1 << 3

// -------- define expire unit ---------
#define UNIT_SECONDS 1
#define UNIT_MILLISECONDS 2


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
    if (!sdsEncodedObject(value)) {
        addReplyErrorLength(c, shared.wrongtypeerr->ptr, sdslen(shared.wrongtypeerr->ptr));
        return C_ERR;
    }
    return C_OK;
}


void getCommand(client *c) {
    getGenericCommand(c);
}

int setGenericCommand(client *c, robj *key, robj *value, int flags, robj *expires, int unit, robj *ok_reply, robj *abort_reply) {
    return C_ERR;
}

void setCommand(client *c) {


    robj *expires;
    int unit = UNIT_SECONDS;
    int flags = SET_OBJECT_NO_FLAG, j;

    for (j=3; j<c->argc; j++) {

        robj *next = j < c->argc ? c->argv[j+1]: NULL;
        sds argv = c->argv[j]->ptr;

        if ((argv[0] == 'n' || argv[0] == 'N') &&
            (argv[1] == 'x' || argv[1] == 'X') && (argv[2] == '\0') && !(flags & SET_OBJECT_XX)) {
            flags |= SET_OBJECT_NX;
        } else if ((argv[0] == 'x' || argv[0] == 'X') &&
                   (argv[1] == 'x' || argv[1] == 'X') && (argv[2] == '\0') && !(flags & SET_OBJECT_NX)) {
            flags |= SET_OBJECT_XX;
        } else if ((argv[0] == 'e' || argv[0] == 'E') &&
                   (argv[1] == 'x' || argv[1] == 'X') && (argv[2] == '\0') && !(flags & SET_OBJECT_PX) && next) {
            flags |= SET_OBJECT_EX;
            expires = next;
            j++;
        } else if ((argv[0] == 'p' || argv[0] == 'E') &&
                   (argv[1] == 'P' || argv[1] == 'X') && (argv[2] == '\0') && !(flags & SET_OBJECT_EX) && next) {
            flags |= SET_OBJECT_PX;
            unit = UNIT_MILLISECONDS;
            expires = next;
            j++;
        } else {
            addReplyErrorLength(c, shared.syntaxerr->ptr, sdslen(shared.syntaxerr->ptr));
        }

    }

    setGenericCommand(c, c->argv[1], c->argv[2], flags, expires, unit, NULL, NULL);

}
