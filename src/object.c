//
// Created by kieren jiang on 2023/6/29.
//

#include "server.h"

int sdsEncodedObject(robj *r) {
    return r->encoding == REDIS_ENCODING_RAW || r->encoding == REDIS_ENCODING_EMBED;
}


robj* createObject(int type, void *ptr) {
    robj *r = zmalloc(sizeof(*r));
    r->refcount = 1;
    r->type = type;
    r->ptr = ptr;
    // todo update lru
    return r;
}

robj* createEmbeddedStringObject(const char *s, size_t len) {

    size_t hdrlen = sizeof(sdshdr);
    robj *r = zmalloc(sizeof(robj) + hdrlen + len + 1);
    sds str = sdsnewlen(s, len);
    r->refcount = 1;
    r->type = REDIS_OBJECT_STRING;
    r->encoding = REDIS_ENCODING_EMBED;
    r->ptr = str;
    return  r;
}

robj* createRawStringObject(const char  *s, size_t len) {
    robj *r = zmalloc(sizeof(robj));
    r->ptr = sdsnewlen(s, len);
    r->type = REDIS_OBJECT_STRING;
    r->encoding = REDIS_ENCODING_RAW;
    r->refcount = 1;
    return r;
}

#define EMBSTR_LIMIT 39
robj* createStringObject(const char *s, size_t len) {

    if (len <= EMBSTR_LIMIT) {
        return createEmbeddedStringObject(s, len);
    } else {
        return createRawStringObject(s, len);
    }
}

robj* createStringObjectFromLongLong(long long value) {
    robj *r = zmalloc(sizeof(*r));
    r->refcount = 1;
    r->type = REDIS_OBJECT_STRING;
    r->encoding = REDIS_ENCODING_INT;
    r->ptr = (void*)value;
    return r;
}

void incrRefCount(robj *o) {
    if (o->refcount == REDIS_SHARED_OBJECT_REF)
        return;
    o->refcount++;
}

void decrRefCount(robj *o) {

    if (o->refcount == REDIS_SHARED_OBJECT_REF)
        return;

    // todo assert  o->refcount >= 1

    if (--o->refcount) {
        return;
    }

    switch (o->type) {
        case REDIS_OBJECT_STRING:
            freeStringObject(o);
            break;
        default:
            // todo panic
            break;
    }

    zfree(o);

}

void freeStringObject(robj *o) {
    if (o->encoding == REDIS_ENCODING_RAW) {
        zfree(o->ptr);
    }
}

void makeObjectShared(robj *o) {
    o->refcount = REDIS_SHARED_OBJECT_REF;
}


robj* tryObjectEncoding(robj *obj) {

    long value;
    size_t len;


    // not safe to encode the object
    if (obj->refcount != 1) return obj;

    if (!sdsEncodedObject(obj)) return obj;

    if (obj->encoding == REDIS_ENCODING_INT) return obj;

    len =  sdslen(obj->ptr);

    // try encoding to long
    if (len <= 20 && string2l((char*)(obj->ptr), len, &value)) {

        if (obj->encoding == REDIS_ENCODING_RAW) sdsfree(obj->ptr);
        obj->ptr = (void*)(value);
        return obj;
    }

    if (obj->encoding == REDIS_ENCODING_RAW && len <= EMBSTR_LIMIT) {
        robj *o = createEmbeddedStringObject(obj->ptr, len);
        decrRefCount(obj);
        return o;
    }

    return obj;

}

robj* getDecodedObject(robj *o) {

    if (sdsEncodedObject(o)) {
        incrRefCount(o);
        return o;
    } else if (o->encoding == REDIS_ENCODING_INT) {
        char buf[32];
        size_t len = ll2string(buf, (long)o->ptr);
        return createStringObject(buf, len);
    } else {
        // todo server panic
        exit(1);
    }

}

int getLongLongFromObjectOrReply(robj *obj, long long *target, client *c, robj *reply) {

    if (getLongLongFromObject(obj, target) == C_ERR) {
        addReply(c, reply);
        return C_ERR;
    }

    return C_OK;
}

int getLongLongFromObject(robj *obj, long long *target) {

    //todo assert obj type is string
    long long value;

    if (obj == NULL)
        value = 0;
    else {
        switch (obj->encoding) {
            case REDIS_ENCODING_INT:
                value = (long)(obj->ptr);
            case REDIS_ENCODING_EMBED:
            case REDIS_ENCODING_RAW:
                if (string2ll(obj->ptr, sdslen(obj->ptr), &value) == 0) return C_ERR;
            default:
                // todo panic
                break;
        }
    }

    if (target) *target = value;

    return C_OK;

}

