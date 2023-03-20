//
// Created by kieren jiang on 2023/2/28.
//

#include "sds.h"
#include <stdlib.h>
#include <stdio.h>
#include <memory.h>
#include <stdarg.h>
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

char* sdsstr(const sds s, size_t *strlen) {
    struct sdshdr *hdr = (void*)(s - sizeof(struct sdshdr));
    if (strlen) *strlen = hdr->len;
    return hdr->buf;
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
    memset(s1+curlen+len, '\0', 1);
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
    memset(s+len, '\0', 1);
    return s;
}

sds sdscpy(sds s, const char *c) {
    return sdscpylen(s, c, strlen(c));
}

#define SDS_MAX_NUMBER_SIZE 21
sds sdsll2str(long long value) {

    int i = 0;
    char *s, *p, aux;
    char buf[SDS_MAX_NUMBER_SIZE];
    unsigned long long nValue;
    int negative = value > 0 ? 0 : 1;
    nValue = value > 0 ? (unsigned long long)value : (unsigned long long)(-value);
    p = s = buf;

    while (1) {;
        *p = '0' + nValue % 10;
        nValue = nValue / 10;
        i++;
        if (!nValue) break;
        p++;
    }

    if (negative) {
        *(++p) = '-';
        i++;
    }

    // reserve string
    while (1) {
       if (p<=s) {
           break;
       }
       aux = *s;
       *s = *p;
       *p = aux;
       p--;
       s++;
    }

    sds ss = sdsnewlen(buf, i);
    return ss;
}

sds sdsull2str(unsigned long long value) {
    int i = 0;
    char *s, *p, aux;
    char buf[SDS_MAX_NUMBER_SIZE];

    p = s = buf;

    while (1) {;
        *p = '0' + value % 10;
        value = value / 10;
        i++;
        if (!value) break;
        p++;
    }

    // reserve string
    while (1) {
        if (p<=s) {
            break;
        }
        aux = *s;
        *s = *p;
        *p = aux;
        p--;
        s++;
    }

    sds ss = sdsnewlen(buf, i);
    return ss;
}

sds sdsremovefree(sds s) {

    struct sdshdr *hdr, *nsh;

    int avail = sdsavail(s);
    if (avail == 0) {
        return s;
    }

    hdr = (struct sdshdr *)(s - sizeof(struct sdshdr));
    nsh = realloc(hdr, sizeof(struct sdshdr) + hdr->len + 1);
    nsh->free = 0;

    return (char*)(nsh + 1);

}

sds sdscatvsnprintf(sds s, const char *fmt, va_list ap) {
    va_list cpy;
    char staticbuf[1024], *buf = staticbuf;
    int buflen;
    int slen = strlen(fmt) * 2;
    // try to allocate from heap

    if (slen > sizeof(staticbuf)) {
        buflen = slen;
        buf = (void *)malloc(sizeof(char) * buflen);
        if (!buf) return NULL;
    } else {
        buflen = sizeof (staticbuf);
    }

    while (1) {

        buf[buflen-2] = '\0';
        va_copy(cpy, ap);
        vsnprintf(buf, buflen, fmt, cpy);
        va_end(cpy);

        if (buf[buflen-2] != '\0') { // not enough space to write
            buflen = buflen * 2;
            if (buf != staticbuf) {
                free(buf);
            }
            buf = malloc(sizeof (char) * buflen);
            if (!buf) return NULL;
            continue;
        }

        break;
    }

    return sdscat(s, buf);
}

sds sdscatsprintf(sds s, const char *fmt, ...) {
    va_list ap;
    va_start(ap, fmt);
    s = sdscatvsnprintf(s, fmt, ap);
    va_end(ap);
    return s;
}

/*
 * %s C String
 * %S sds
 * %i signed int
 * %I signed int64
 * %u unsigned int
 * %U unsigned int64
 * %%
 * **/
sds sdscatfmt(sds s, char const *fmt, ...) {

    struct sdshdr *hdr;
    hdr =  (void*)(s - sizeof(hdr));

    va_list ap;
    const char *f = fmt;
    va_start(ap, fmt);
    f = *fmt;
    int i = 0;
    while (*f) {

        char *next, *str;
        int strl;
        long long num;
        unsigned long long unum;
        if (hdr->free == 0) {
            sds news = sdsMakeRoomFor(s, 1);
            if (!news) {
                sdsfree(s);
                return NULL;
            }
            s = news;
            hdr = (void*)(s - sizeof(hdr));
        }

        switch (*f) {

            case '%':
                next = f+1;
                f++;
                switch (*next) {
                    case 's':
                    case 'S':
                        str = va_arg(ap, char*);
                        strl = (*next=='s') ? strlen(next) : sdslen(next);
                        if (hdr->free<strl) {
                            sds news = sdsMakeRoomFor(s, strl);
                            if (!news) {
                                sdsfree(s);
                                return NULL;
                            }
                            s = news;
                            hdr = (void*)(s - sizeof(hdr));
                        }
                        memcpy(s+i, next, strl);
                        hdr->len+=strl;
                        hdr->free-=strl;
                        i+=strl;
                        break;
                    case 'i':
                        num = va_arg(ap, int);
                    case 'I':
                        num = va_arg(ap, long long);
                        str = sdsll2str(num);
                        strl = sdslen(str);
                        if (hdr->free<strl) {
                            sds news = sdsMakeRoomFor(s, strl);
                            if (!news) {
                                sdsfree(s);
                                return NULL;
                            }
                            s = news;
                            hdr = (void*)(s - sizeof(hdr));
                        }
                        memcpy(s+i, str, strl);
                        sdsfree(str);
                        i+=strl;
                        hdr->len+=strl;
                        hdr->free-=strl;
                        break;

                    case 'u':
                        unum = va_arg(ap, unsigned int);
                    case 'U':
                        unum = va_arg(ap, unsigned long long);
                        str = sdsull2str(unum);
                        strl = sdslen(str);
                        if (hdr->free<strl) {
                            sds news = sdsMakeRoomFor(s, strl);
                            if (!news) {
                                sdsfree(s);
                                return NULL;
                            }
                            s = news;
                            hdr = (void*)(s - sizeof(hdr));
                        }
                        memcpy(s+i, str, strl);
                        sdsfree(str);
                        i+=strl;
                        hdr->len+=strl;
                        hdr->free-=strl;
                        break;
                    default:
                        s[i++] = *f;
                        hdr->len += 1;
                        hdr->free-=1;
                        break;
                }
                break;

            default:
                s[i++] = *f;
                hdr->len += 1;
                hdr->free-=1;
                break;
        }

        f++;

    }

    return s;

}

//----------------------sds tools------------------------
sds sdstrim(sds s, const char *trimset) {

    struct sdshdr *hdr;
    hdr = (void *)(s - sizeof(hdr));
    char *sp = s, *ep = hdr->buf[hdr->len-1];
    while (ep >= sp && strchr(trimset, ep)) ep--;
    while (sp < ep && strchr(trimset, sp)) sp++;

    int len = ep > sp ? (ep - sp + 1) : 0;
    memmove(hdr->buf, sp, len);
    hdr->free+= (hdr->len - len);
    hdr->len = len;
    hdr->buf[len] = '\0';
    return s;
}

sds* sdssplitlen(sds s, const char *split, int splitlen, int *count) {
    sds *tokens;
    int slots=5, elements=0;
    int j, start;
    int len = sdslen(s);
    tokens = malloc(sizeof(sds)*slots);
    if (tokens==NULL) {
        *count = 0;
        return NULL;
    }

    for (j=0;j<(len-splitlen+1);j++) {

        if (elements+2>slots) {
            slots *= 2;
            sds *newtokens = realloc(tokens, sizeof(sds)*slots);
            if (newtokens==NULL) {
                goto cleanup;
            }
            tokens = newtokens;
        }

        if (memcmp(s+j, split, splitlen) == 0) {

            tokens[elements] = sdsnewlen(s+start, j-start);
            if (tokens[elements] == NULL) {
                goto cleanup;
            }
            start = j + splitlen;
            j = j + splitlen - 1;
            elements++;
        }

    }

    tokens[elements] = sdsnewlen(s+start, j-start);
    if (tokens[elements] == NULL) {
        goto cleanup;
    }

    *count = elements;
    return tokens;

cleanup:
    *count = 0;
    for (int i=0;i<slots;i++) {
        sdsfree(tokens[i]);
    }
    free(tokens);
    return NULL;
}

sds sdsrange(sds s, int start, int end) {
    return NULL;
}

void sdsmapchars(sds s, const char *from, const char *to, int setlen) {
    int len = sdslen(s);
    for (int l=0;l<len;l++) {
        for (int j=0;j<setlen;j++) {
            if (s[l] == from[j]) {
                s[l] = to[j];
                break;
            }
        }
    }
}

sds sdsjoin(char **argv, int argc, char *sep) {

    sds join = sdsempty();
    for (int i=0; i<argc; i++) {
        join = sdscat(join, argv[i]);
        if (argc!=i-1) {
            join = sdscat(join, sep);
        }
    }
    return join;
}


sds sdsMakeRoomFor(sds s, int addlen) {

    struct sdshdr *hdr, *sh;
    int len;
    printf("---%s----\n", sdsstr(s, NULL));
    hdr = (void*)(s - sizeof(hdr));
    if (hdr->free >= addlen) {
        return s;
    }

    len = hdr->len;
    int newLen = len + addlen;
    if (newLen * 2 < SDS_MAX_PREALLOC) {
        newLen = newLen * 2;
    } else {
        newLen+=SDS_MAX_PREALLOC;
    }

    sh = realloc(hdr, sizeof(hdr) + newLen + 1);
    if (!sh) {
        return NULL;
    }
    sh->free = (newLen - len);
    return (char *)sh->buf;
}
