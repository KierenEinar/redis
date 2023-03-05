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
    if (newLen < SDS_MAX_PREALLOC) {
        newLen = newLen<<1;
    } else {
        newLen+=SDS_MAX_PREALLOC;
    }

    sh = (void *)realloc(hdr, sizeof(hdr) + newLen);
    sh->len = len;
    sh->free = newLen - len;
    return (char *)sh->buf;
}
