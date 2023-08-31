//
// Created by kieren jiang on 2023/5/28.
//

#include <memory.h>
#include <stdarg.h>

#include "sds.h"
#include "zmalloc.h"
#include "utils.h"

sds sdsnew(const char *c) {
    return sdsnewlen(c, strlen(c));
}

sds sdsnewlen(const char *c, size_t len) {

    sdshdr *sh;
    sh = zmalloc(sizeof(sdshdr) + len + 1);
    sds s = (char*)(sh) + sizeof(sdshdr);
    sh->used = len;
    sh->free = 0;
    if (c && len) {
        memcpy(s, c, len);
    } else {
        memset(s, 0, len);
    }
    s[len] = '\0';
    return s;

}

void sdsfree(sds s) {
    sdshdr *sh;
    sh = (sdshdr*)(s - sizeof(*sh));
    zfree(sh);
}

sds sdsdup(sds s) {
    return sdsnewlen((char*)s, sdslen(s));
}

sds sdsempty() {
    return sdsnewlen(NULL, 0);
}

sds sdsclear(sds s) {
    return NULL;
}

sds sdsMakeRoomFor(sds s, size_t len) {

    if (sdsavail(s) >= len) {
        return s;
    }

    sdshdr *sh;
    sh = (sdshdr*)((char*)s - sizeof(sdshdr));

    size_t reallen = sh->used + len;

    if (reallen < SDS_PREALLOC) {
        reallen *= 2;
    } else {
        reallen +=SDS_PREALLOC;
    }
    sh->free = reallen - sh->used;
    zrealloc(sh, sizeof(*sh) + reallen + 1);
    return sh->buf;
}
sds sdscatlen(const char *c, size_t len) {
    return NULL;
}
sds sdscatsds(sds dest, sds src) {

    size_t avail, dlen, slen;
    sdshdr *dhdr;

    avail = sdsavail(dest);
    dlen = sdslen(dest);
    slen = sdslen(src);
    if (avail < slen) {
       dest = sdsMakeRoomFor(dest, slen);
    }

    memcpy(dest + dlen, src, slen);
    sdsincrlen(dest, slen);
    return dest;
}
size_t sdslen(sds s) {
    sdshdr *sh;
    sh = (sdshdr*)((char*)s - sizeof(sdshdr));
    return sh->used;
}

void sdsincrlen(sds s, size_t len) {
    sdshdr *sh;
    sh = (sdshdr*)((char*)s - sizeof(sdshdr));
    sh->used+=len;
}

size_t sdsavail(sds s) {
    sdshdr *sh;
    sh = (sdshdr *)((char *)s - sizeof(*sh));
    return sh->free;
}

sds sdscatfmt(sds s, const char *fmt, ...) {

    const char *f = fmt;
    size_t initlen = sdslen(s);
    size_t i = initlen;
    va_list ap;
    va_start(ap, fmt);

    while (*f) {

        long long num;
        unsigned long long unum;
        char next;
        char *str;
        size_t slen;
        switch (*f) {
            case '%':
                next = *(++f);
                switch (next) {
                    case 's':
                    case 'S':
                        str = va_arg(ap, char*);
                        slen = strlen(str);
                        if (sdsavail(s) < slen) {
                            s = sdsMakeRoomFor(s, slen);
                        }
                        memcpy(s+i, str, slen);
                        sdsincrlen(s, slen);
                        i+=slen;
                        break;
                    case 'i':
                    case 'I':
                        if (next == 'i')
                            num = va_arg(ap, int);
                        else
                            num = va_arg(ap, long long);

                        {
                            char buf[21];
                            size_t size = ll2string(buf, num);
                            if (sdsavail(s) < size) {
                                s = sdsMakeRoomFor(s, size);
                            }
                            memcpy(s+i, buf, size);
                            sdsincrlen(s, size);
                            i+=size;
                        }
                        break;
                    case 'u':
                    case 'U':
                        if (next == 'u')
                            num = va_arg(ap, unsigned int);
                        else
                            num = va_arg(ap, unsigned long long);

                        {
                            char buf[21];
                            size_t size = ull2string(buf, num);
                            if (sdsavail(s) < size) {
                                s = sdsMakeRoomFor(s, size);
                            }
                            memcpy(s+i, buf, size);
                            sdsincrlen(s, size);
                            i+=size;
                        }
                        break;
                    default:
                        if (sdsavail(s) < 1) {
                            s = sdsMakeRoomFor(s, 1);
                        }
                        s[i++] = next;
                        sdsincrlen(s, 1);
                        break;
                }
                f++;
                break;

            default:
                if (sdsavail(s)==0) {
                    s = sdsMakeRoomFor(s, 1);
                }
                s[i++] = *f;
                sdsincrlen(s, 1);
                f++;
        }

    }

    va_end(ap);
    return s;

}
sds sdscatprintf(sds s, const char *fmt, ...) {

    va_list list;
    va_start(list, fmt);
    s = sdscatvnprintf(s, fmt, list);
    va_end(list);
    return s;
}
sds sdscatvnprintf(sds s, const char *fmt, va_list list) {
    return NULL;
}

sds sdsrange(sds s, long start, long end) {

    sdshdr *sh;
    size_t slen;
    long trim;

    sh = (sdshdr*)((char*)s - sizeof(sdshdr));
    slen = sh->used;

    if (start < 0)
        start = start + (long)slen;

    if (end < 0)
        end = end + (long)slen;

    if (start < 0)
        start = 0;

    if (end >= slen)
        end = (long)slen - 1;

    if (start >= end || start >= slen)
        return s;

    trim = end - start + 1;

    if (trim * 2 < sh->used + sh->free) {
        sds news = sdsnewlen(NULL, trim * 2);
        memcpy(news, sh->buf+start, trim);
        news[trim] = '\0';
        sdsfree(s);
        return news;
    }

    memmove(s, sh->buf+start, trim);
    sh->free = sh->free + sh->used - trim;
    sh->used = trim;
    sh->buf[trim] = '\0';
    return s;
}