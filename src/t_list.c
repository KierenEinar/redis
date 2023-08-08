//
// Created by kieren jiang on 2023/8/7.
//

#include "server.h"

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

    robj *lobj = lookupKeyWrite(c, c->argv[1]);
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

    robj *lobj = lookupKeyWrite(c, c->argv[1]);
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
        dictDelete(c->db->dict, c->argv[1]->ptr);
        removeExpire(c, c->argv[1]);
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