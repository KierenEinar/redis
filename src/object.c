//
// Created by kieren jiang on 2023/3/16.
//

#include "server.h"
#include <stdlib.h>
#include <limits.h>
#include <memory.h>

void incrRef(robj *o) {
    if (o->refcount != INT_MAX) // int_max represents the share object
        o->refcount++;
}

robj* createObject(int type, void *ptr) {

    robj *o = malloc(sizeof (*o));
    o->type = type;
    o->refcount = 0;
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

void decrRefCount(robj *o) {
    if (o->refcount == 1) {
        switch (o->type) {
            case OBJECT_STRING:
                freeStringObject(o);
                break;
        }
        free(o);
    }
    o->refcount--;
}

void freeStringObject(robj *o) {
    if (o->encoding == OBJECT_ENCODING_RAW) {
        sdsfree(o->ptr);
    }
}

robj* createEmbeddedStringObject(const char *s, size_t len) {

    robj *o = malloc(sizeof(robj) + sizeof(struct sdshdr) + len + 1);

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


robj* tryObjectEncoding(robj *obj) {

    if (obj->type != OBJECT_STRING) {
        return obj;
    }

    if (obj->encoding != OBJECT_ENCODING_RAW && obj->encoding != OBJECT_ENCODING_EMBSTR) {
        return obj;
    }

    // not safe to try object encode if there is another reference
    if (obj->refcount > 1) {
        return obj;
    }

    sds buf = (char *)obj->ptr;
    int len = sdslen(buf);

    long value;

    // convert to int
    if (len <= STRING_INT_LIMIT_LEN && string2l(buf, len, &value)) {
        obj->encoding = OBJECT_ENCODING_INT;
        if (obj->encoding == OBJECT_ENCODING_RAW) {
            free(obj->ptr);
        }
        obj->ptr = (void*)value;
        return obj;
    }


    // try raw to embedstr
    if (obj->encoding == OBJECT_ENCODING_EMBSTR) {
        return obj;
    }

    if (len <= EMBSTR_LEN_LIMIT) {
        sds s = obj->ptr;
        createEmbeddedStringObject(s, sdslen(s));
    }


}


