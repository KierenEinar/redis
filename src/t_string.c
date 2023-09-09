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


int expireIfNeed(redisDb *db, robj *key) {
    dictEntry *de;
    int64_t expired;
    if (dictSize(db->expires) == 0) return 0;
    de = dictFind(db->expires, key->ptr);
    if (de == NULL) return 0;
    expired = dictGetSignedInteger(de);
    mstime_t ms = mstime();
    if (expired >= ms) return 0;
    return dbSyncDelete(db, key);
}

robj *lookupKey(redisDb *db, robj *key) {
    long long expired;
    if (dictSize(db->dict) == 0) return NULL;

    robj *value = dictFetchValue(db->dict, key->ptr);
    expired = getExpire(db, (robj*)key);

    if (value && expired > 0 && expired < mstime()) {
        dictDelete(db->dict, key->ptr);
        dictDelete(db->expires, key->ptr);
        value = NULL;
    }

    return value;
}

robj *lookupKeyRead(redisDb *db, robj *key) {
    expireIfNeed(db, key);
    return lookupKey(db, key);
}

robj *lookupKeyWrite(redisDb *db, robj *key) {
    expireIfNeed(db, key);
    return lookupKey(db, key);
}

robj *lookupKeyReadOrReply(client *c, robj *key, robj *reply) {
    expireIfNeed(c->db, key);
    robj *val = lookupKey(c->db, key);
    if (!val) {
        if (reply) {
            // todo send reply to client
        } else {
            addReply(c, shared.nullbulk);
        }
    }
    return val;
}

int getGenericCommand(client *c) {
    robj *value = lookupKeyReadOrReply(c, c->argv[1],NULL);
    if (value == NULL) return C_ERR;
    if (!sdsEncodedObject(value)) {
        addReply(c, shared.wrongtypeerr);
        return C_ERR;
    }
    addReplyBulk(c, value);
    return C_OK;
}

void selectCommand(client *c) {
    int dbid = atoi(c->argv[1]->ptr);
    if (dbid < 0 || dbid > server.dbnum) {
        addReply(c, shared.syntaxerr);
        return;
    }
    selectDb(c, dbid);
}

void getCommand(client *c) {
    getGenericCommand(c);
}

int setGenericCommand(client *c, robj *key, robj *value, int flags, robj *expires, int unit, robj *ok_reply, robj *abort_reply) {

    long long expire = 0;

    if (expires) {

        if (getLongLongFromObjectOrReply(expires, &expire, c, abort_reply) == C_ERR) {
            addReplyError(c, "integer invalid or out of bounds");
            return C_ERR;
        }

        if (expire <= 0) {
            addReplyError(c, "invalid expire time");
            return C_ERR;
        }

        if (unit == UNIT_SECONDS) {
            expire *= 1000;
        }
    }

    if (((flags & SET_OBJECT_NX) && lookupKeyWrite(c->db, key)) ||
            ((flags & SET_OBJECT_XX) && !lookupKeyWrite(c->db, key))) {
        addReply(c, abort_reply ? abort_reply : shared.nullbulk);
        return C_ERR;
    }

    setKey(c, key, value);
    if (expire) setExpire(c, key, expire);
    addReply(c, ok_reply ? ok_reply :shared.ok);
    server.dirty++;
    return C_OK;
}

void setCommand(client *c) {

    robj *expires = NULL;
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
        } else if ((argv[0] == 'p' || argv[0] == 'P') &&
                   (argv[1] == 'x' || argv[1] == 'X') && (argv[2] == '\0') && !(flags & SET_OBJECT_EX) && next) {
            flags |= SET_OBJECT_PX;
            unit = UNIT_MILLISECONDS;
            expires = next;
            j++;
        } else {
            addReply(c, shared.syntaxerr);
            return;
        }

    }

    setGenericCommand(c, c->argv[1], c->argv[2], flags, expires, unit, NULL, NULL);

}

void expireCommand(client *c) {
    expireGenericCommand(c, UNIT_SECONDS);
}

void pexpireCommand(client *c) {
    expireGenericCommand(c, UNIT_MILLISECONDS);
}

void expireGenericCommand(client *c, int unit) {
    robj *ttl;
    long long when;

    ttl = getDecodedObject(c->argv[2]);
    if (!string2ll(ttl->ptr, sdslen(ttl->ptr), &when)) {
        addReplyError(c, "integer invalid or out of bounds");
        return;
    }

    if (when < 0) {
        addReplyError(c, "invalid expire time");
        return;
    }

    if (!lookupKeyReadOrReply(c, c->argv[1], shared.czero)) {
        return;
    }

    if (unit == UNIT_SECONDS) {
        when *= 1000;
    }

    setExpire(c, c->argv[1], when);
    addReply(c, shared.cone);
    signalKeyAsModified(c->db, c->argv[1]);
}


void msetCommand(client *c) {
    msetGenericCommand(c, 0);
}

void mgetCommand(client *c) {

    int j;
    addReplyMultiBulkLen(c, c->argc-1);
    robj *value;
    for (j=1; j<c->argc; j++) {
        if ((value = lookupKeyRead(c->db, c->argv[j])) == NULL) {
            addReply(c, shared.nullbulk);
        } else {
            addReplyBulk(c, value);
        }
    }
}

// nx -> :-1 or :1
// only set -> +ok, $-1
int msetGenericCommand(client *c, int nx) {

    int j, busykeys = 0;

    if (c->argc % 2 == 0) {
        addReply(c, shared.syntaxerr);
        return C_ERR;
    }

    if (nx) {
        for (j=1; j<c->argc; j+=2) {
            if (lookupKeyWrite(c->db, c->argv[j])) {
                busykeys++;
            }
        }
        if (busykeys) {
            addReply(c, shared.czero);
            return C_ERR;
        }
    }

    for (j=1; j<c->argc; j+=2) {
        c->argv[j+1] = tryObjectEncoding(c->argv[j+1]);
        setKey(c, c->argv[j], c->argv[j+1]);
    }

    addReply(c, nx ? shared.cone : shared.ok);
    server.dirty+=(c->argc - 1) /2;
    return C_OK;
}