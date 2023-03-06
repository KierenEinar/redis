//
// Created by kieren jiang on 2023/2/28.
//

#include "sds.h"
#include <stdlib.h>
#include <stdio.h>
#include <memory.h>
sds sdsnewlen(const char *init, int len) {
    struct sdshdr *sdshdr = (void* )malloc(sizeof(struct sdshdr) + len +1);
    if (!sdshdr) {
        return NULL;
    }

    sdshdr->len = len;
    sdshdr->free = 0;
    if (init && len) {
        memcpy(sdshdr->buf, init, len);
    }
    sdshdr->buf[len] = '\0';
    return (char *)sdshdr->buf;
}

sds sdsnew(const char *c) {
    return sdsnewlen(c, strlen(c));
}

sds sdsdup(sds s) {
    return sdsnewlen(s, sdslen(s));
}

int sdslen(const sds s) {
    struct sdshdr *hdr = (void*)(s - sizeof(struct sdshdr));
    return hdr->len;
}

int sdsavail(const sds s) {
    struct sdshdr *hdr = (void*)(s - sizeof(struct sdshdr));
    return hdr->free;
}

sds sdsrepeat(const char *c, int len) {
   if (c && len) {
       int clen = strlen(c);
       int rlen = clen * len;
       char *r = (char *)malloc(rlen);
       if (!r) {
           return NULL;
       }
       for (int i=0;i<len;i++) {
           memcpy(r+clen*i, c, clen);
       }
       sds s = sdsnewlen(r, rlen);
       free(r);
       return s;
   }
   return sdsempty();
}

sds sdsempty(void) {
    return sdsnewlen("", 0);
}

void sdsfree(sds s) {
    struct sdshdr *sdshdr;
    sdshdr = (void *)(s - sizeof(sdshdr));
    free(sdshdr);
}

void sdsclear(sds s) {
    struct sdshdr *sdshdr;
    sdshdr = (void *)(s - sizeof(sdshdr));
    sdshdr->free += sdshdr->len;
    sdshdr->len = 0;
    sdshdr->buf[0] = '\0';
}

sds sdscatlen(sds s, const char *c, int len) {

    struct sdshdr *hdr;
    s = sdsMakeRoomFor(s, len);
    if (s == NULL) return NULL;
    hdr = (void *)(s - sizeof(hdr));
    memcpy(s+hdr->len, c, len);
    hdr->len+=len;
    hdr->free-=len;
    hdr->buf[hdr->len] = '\0';
    return s;
}

sds sdscat(sds s, const char *c) {
    return sdscatlen(s, c, strlen(c));
}

sds sdscatsds(sds s1, const sds s2) {

    struct sdshdr *hdr;
    int len = sdslen(s2);
    int curlen = sdslen(s1);
    s1 = sdsMakeRoomFor(s1, len);
    if (!s1) return NULL;
    memcpy(s1+curlen, s2, len);
    memset(s1+curlen+1, '\0', 1);
    hdr = (void *)(s1 - sizeof(hdr));
    hdr->len += len;
    hdr->free-=len;
    return s1;
}

sds sdscpylen(sds s, const char *c, int len) {
    struct sdshdr *hdr;
    int curlen = sdslen(s);
    if (curlen < len) {
        s = sdsMakeRoomFor(s, len-curlen);
        if (!s) return NULL;
    }
    hdr = (void *)(s - sizeof(hdr));
    memcpy(s, c, len);
    hdr->len = len;
    hdr->free += (len - curlen);
    memset(s+len+1, '\0', 1);
    return s;
}

sds sdscpy(sds s, const char *c) {
    return sdscpylen(s, c, strlen(c));
}


sds sdsMakeRoomFor(sds s, int addlen) {

    struct sdshdr *hdr, *sh;
    int len, free;
    hdr = (void*)(s - sizeof(struct sdshdr));
    if (hdr->free >= addlen) {
        return s;
    }

    len = hdr->len;
    free = hdr -> free;

    int newLen = len + addlen;
    if (newLen * 2 < SDS_MAX_PREALLOC) {
        newLen = newLen * 2;
    } else {
        newLen+=SDS_MAX_PREALLOC;
    }

    sh = (void *)realloc(hdr, sizeof(hdr) + newLen);
    if (!sh) {
        return NULL;
    }

    sh->len = len;
    sh->free = newLen - len;
    return (char *)sh->buf;
}
