//
// Created by kieren jiang on 2023/3/16.
//

#include "server.h"
#include <stdlib.h>
#include <limits.h>
#include <memory.h>

void incrRef(robj *o) {
    if (o->refcount != OBJ_SHARED_REFCOUNT) // int_max represents the share object
        o->refcount++;
}

void decrRefCount(robj *o) {
    if (o->refcount == OBJ_SHARED_REFCOUNT)
        return;

    if (o->refcount == 1) {
        switch (o->type) {
            case OBJECT_STRING:
                freeStringObject(o);
                break;
            default:
                //todo panic
                break;
        }
        free(o);
    }
    o->refcount--;
}

void makeObjectShared(robj *o) {
    if (o->refcount > 1) return;
    o->refcount = OBJ_SHARED_INTEGERS;
}

robj* createObject(int type, void *ptr) {

    robj *o = malloc(sizeof (*o));
    o->type = type;
    o->refcount = 1;
    o->encoding = OBJECT_ENCODING_RAW;
    o->ptr = ptr;
    // todo add lru clock
    //  o->lru = getLruClock();
    return o;
}

robj* createStringObject(const char *s, size_t len) {
    if (len <= EMBSTR_LEN_LIMIT) {
        return createEmbeddedStringObject(s, len);
    } else {
        return createRawStringObject(s, len);
    }
}

robj* createRawStringObject(const char *s, size_t len) {
    return createObject(OBJECT_STRING, sdsnewlen(s, len));
}


void freeStringObject(robj *o) {
    if (o->encoding == OBJECT_ENCODING_RAW) {
        sdsfree(o->ptr);
    }
}

robj* createEmbeddedStringObject(const char *s, size_t len) {

    robj *o = malloc(sizeof(*o) + sizeof(struct sdshdr) + len + 1);

    struct sdshdr *hdr = (void*)(o+1);
    o->encoding = OBJECT_ENCODING_EMBSTR;
    o->type = OBJECT_STRING;
    o->refcount = 1;

    hdr->len = len;
    hdr->free = 0;
    if (s) {
        memcpy(hdr->buf, s, len);
        hdr->buf[len] = '\0';
    } else {
        memset(hdr->buf, 0, len+1);
    }

    // todo add lru clock
    return o;
}

// for saving space
robj* tryObjectEncoding(robj *obj) {

    if (obj->type != OBJECT_STRING) {
        return obj;
    }

    if (!sdsEncodedObject(obj)) {
        return obj;
    }

    // not safe to try object encode if there is another reference
    if (obj->refcount > 1) {
        return obj;
    }

    sds buf = obj->ptr;
    int len = sdslen(buf);

    long value;

    // convert to int
    if (len <= STRING_INT_LIMIT_LEN && string2l(buf, len, &value)) {

        if (value >= 0 && value < OBJ_SHARED_INTEGERS) {
            decrRefCount(obj);
            return shared.integers[value];
        }

        if (obj->encoding == OBJECT_ENCODING_RAW)
            free(obj->ptr);

        obj->encoding = OBJECT_ENCODING_INT;
        obj->ptr = (void*)value;
        return obj;
    }

    if (len <= EMBSTR_LEN_LIMIT) {
        if (obj->encoding == OBJECT_ENCODING_EMBSTR)
            return obj;

        robj *emb = createEmbeddedStringObject(buf, len);
        decrRefCount(obj);
        return emb;
    }

    // try save space for remove free space

    if (obj->encoding == OBJECT_ENCODING_RAW && sdsavail(buf) > len / 10) {
        obj->ptr = sdsRemoveFree(buf);
    }

    return obj;
}

robj* getDecodedObject(robj *o) {

    if (sdsEncodedObject(o)) {
        incrRef(o);
        return o;
    }

    if (o->encoding == OBJECT_ENCODING_INT) {
        long v = (long)o->ptr;
        char s[STRING_INT_LIMIT_LEN];
        size_t len = ll2string(s, sizeof(s), v);
        robj *emb= createEmbeddedStringObject(s, len);
        return emb;
    } else {
        //todo panic
    }

}


int getLongLongFromObject(robj *obj, long long *target) {

    if (!obj) return -1;

    long long value = 0;

    if (obj->type != OBJECT_STRING) return -1;

    if (obj->encoding == OBJECT_ENCODING_INT) {
        value = (long)obj->ptr;
        if(target) *target = value;
        return 1;
    } else if (obj->encoding == OBJECT_ENCODING_RAW || obj->encoding == OBJECT_ENCODING_EMBSTR) {
        sds s = obj->ptr;
        if (string2ll(s, sdslen(s), &value)) {
            if (target) *target = value;
            return 1;
        }
    } else {
        // todo panic
    }

    return -1;

}

robj* createStringObjectFromLongLong(long long value) {
    char s[STRING_INT_LIMIT_LEN];
    size_t len = ll2string(s, STRING_INT_LIMIT_LEN, value);
    return createStringObject(s, len);
}


int getLongFromObject(robj *obj, long *target) {

    long long value = 0;

    if (!getLongLongFromObject(obj, &value)) {
        return -1;
    }

    if (value < LONG_MIN || value > LONG_MAX) {
        return -1;
    }

    if (target) *target = (long)value;
    return 1;
}

