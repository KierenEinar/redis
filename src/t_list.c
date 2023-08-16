//
// Created by kieren jiang on 2023/8/7.
//

#include "server.h"

typedef struct listTypeIterator{
    robj *subject;
    unsigned char encoding;
    int direction;
    quicklistIter *iter;
}listTypeIterator;

typedef struct listTypeEntry{
    quicklistEntry *entry;
}listTypeEntry;

listTypeIterator *listTypeGetIterator(robj *subject, long long idx, int direction) {

    listTypeIterator *iter = NULL;

    if (subject->encoding == REDIS_ENCODING_LIST) {

        iter = zmalloc(sizeof(*iter));
        iter->subject = subject;
        iter->direction = direction;
        int where = (direction == LIST_ITER_DIR_FORWARD) ? QUICK_LIST_HEAD : QUICK_LIST_TAIL;
        iter->iter = quicklistIteratorAtIndex(subject->ptr, idx, where);

    } else {
        // todo server panic
    }

    return iter;

}

int listTypeNext(listTypeIterator *iter, listTypeEntry *entry) {
    return 0;
}

void listTypeReleaseIter(listTypeIterator *iter) {

}

void lrangeCommand(client *c) {

    long long start, end, llen, rangelen;

    robj *subject;
    if ((subject=lookupKeyReadOrReply(c, c->argv[1], shared.emptymultibulk)) == NULL
        || checkType(subject, REDIS_OBJECT_LIST)) {
        return;
    }

    if (getLongLongFromObjectOrReply(c->argv[2], &start, c, NULL) != C_OK
        || getLongLongFromObjectOrReply(c->argv[3], &end, c, NULL) != C_OK) {
        return;
    }

    llen = listTypeLen(c->argv[1]);

    if (start<0) start = llen + start;
    if (end<0) end = llen + end;
    if (start<0) start = 0;
    if (end >= llen) end = llen - 1;

    if (start > end || start > llen) {
        addReply(c, shared.emptymultibulk);
        return;
    }

    rangelen = end - start;

    if (subject->encoding == REDIS_ENCODING_LIST) {

        listTypeIterator *iter = listTypeGetIterator(subject, start, LIST_ITER_DIR_FORWARD);

        addReplyMultiBulkLen(c, rangelen);

        while (rangelen--) {
            listTypeEntry entry;
            listTypeNext(iter, &entry);
            if (entry.entry->str) {
                addReplyLongLongPrefix(c, entry.entry->size, '$');
                addReplyString(c, (const char *)entry.entry->str, entry.entry->size);
            } else {
                addReplyLongLong(c, entry.entry->llvalue);
            }
        }

        listTypeReleaseIter(iter);

    } else {
        // todo server panic
    }
}


void lpushCommand(client *c) {
    pushGenericCommand(c, LIST_HEAD);
}

void rpushCommand(client *c) {
    pushGenericCommand(c, LIST_TAIL);
}

void lpopCommand(client *c) {
    popGenericCommand(c, LIST_HEAD);
}

void rpopCommand(client *c) {
    popGenericCommand(c, LIST_TAIL);
}

void pushGenericCommand(client *c, int where) {

    robj *lobj = lookupKeyWrite(c->db, c->argv[1]);
    if (lobj && !checkType(lobj, REDIS_OBJECT_LIST)) {
        addReply(c, shared.wrongtypeerr);
        return;
    }

    for (int j=2; j<c->argc; j++) {
        if (!lobj) {
            lobj = createListTypeObject();
            dbAdd(c, c->argv[1], lobj);
        }
        listTypePush(lobj, c->argv[j], where);
    }

    addReplyLongLong(c, listTypeLen(lobj));
}

void popGenericCommand(client *c, int where) {

    robj *lobj = lookupKeyWrite(c->db, c->argv[1]);
    if (!lobj) {
        addReply(c, shared.nullbulk);
        return;
    }

    if (!checkType(lobj, REDIS_OBJECT_LIST)) {
        addReplyBulk(c, shared.wrongtypeerr);
        return;
    }

    robj *value = listTypePop(lobj, where);
    addReplyBulk(c, value);
    decrRefCount(value);

    if (!listTypeLen(lobj)) {
        quicklistRelease(lobj->ptr);
        removeExpire(c, c->argv[1]);
        dictDelete(c->db->dict, c->argv[1]->ptr);
    }


}

void listTypePush(robj *subject, robj *value, int where) {

    if (subject->encoding == REDIS_ENCODING_LIST) {
        value = getDecodedObject(value);
        quicklistPush(subject->ptr, value->ptr, sdslen(value->ptr), where == LIST_HEAD ? QUICK_LIST_HEAD :QUICK_LIST_TAIL);
        decrRefCount(value);
    } else {
        // todo server panic
    }
}

robj* listTypePop(robj *subject, int where) {

    robj *value = NULL;
    long long llvalue = -123456789;

    if (subject->encoding == REDIS_ENCODING_LIST) {

        quicklistPopCustom(subject->ptr, where == LIST_HEAD ? QUICK_LIST_HEAD : QUICK_LIST_TAIL,
                           (void **)&value, NULL, &llvalue, listPopSaver);
        if (!value) {
            value = createStringObjectFromLongLong(llvalue);
        }
        return value;

    } else {
        // todo server panic
        return NULL;
    }

}

void* listPopSaver(void *data, unsigned int size) {
    return createStringObject((char*)data, size);
}

unsigned int listTypeLen(robj *lobj) {

    if (lobj->encoding == REDIS_ENCODING_LIST) {
        return quicklistCount(lobj->ptr);
    } else {
        // server panic
        return 0;
    }
}

// pop an element from head, when not exists, block the client with options.
void blpopCommand(client *c) {
    blockingGenericCommand(c, QUICK_LIST_TAIL);
}

// pop and element from tail, when not exists, block the client with options.
void brpopCommand(client *c) {
    blockingGenericCommand(c, QUICK_LIST_HEAD);
}

void blockingGenericCommand(client *c, int where) {

    long long timeout;
    int j;

    if (getTimeoutFromObjectOrReply(c, c->argv[c->argc-1], UNIT_SECONDS, &timeout, NULL) != C_OK) {
        return;
    }

    for (j=1; j<c->argc-1; j++) {
        robj *o = lookupKeyWrite(c->db, c->argv[j]);
        if (o != NULL) {
            if (o->type != REDIS_OBJECT_LIST) {
                addReply(c, shared.wrongtypeerr);
                return;
            }

            robj *val = listTypePop(o, where);

            if (!listTypeLen(o)) {
                quicklistRelease(o->ptr);
                removeExpire(c, c->argv[1]);
                dictDelete(c->db->dict, c->argv[1]->ptr);
            }

            if (val != NULL) {
                addReplyMultiBulkLen(c, 2);
                addReplyBulk(c, c->argv[j]);
                addReplyBulk(c, val);
                return;
            }

        }
    }

    blockForKeys(c, &c->argv[1], c->argc - 2, timeout);

}

void signalListAsReady(redisDb *db, robj *key) {

    readyList *rl;

    if (dictFind(db->blocking_keys, key) == NULL) return;
    if (dictFind(db->ready_keys, key) != NULL) return;


    rl = zmalloc(sizeof(*rl));
    rl->key = key;
    rl->db = db;
    incrRefCount(rl->key);
    listAddNodeTail(server.ready_keys, rl);

    incrRefCount(key);
    dictAdd(db->ready_keys, key, NULL);

}