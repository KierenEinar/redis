//
// Created by kieren jiang on 2023/2/28.
//

#ifndef REDIS_SDS_H
#define REDIS_SDS_H

#include <stdarg.h>
#include <sys/types.h>
#define SDS_MAX_PREALLOC 1 << 20 // 1m

typedef char *sds;

struct __attribute__((__packed__)) sdshdr {
    int len;
    int free;
    char buf[];
};

//---------------------------------API--------------------------------

sds sdsnewlen(const char *c, int len);
sds sdsnew(const char *c);
sds sdsdup(const sds s);
sds sdsrepeat(const char *c, int repeat);
sds sdsempty(void);
void sdsfree(sds s);
void sdsclear(sds s);
sds sdscatlen(sds s, const char *c, int len);
sds sdscat(sds s1, const char *c);
sds sdscatsds(sds s1, const sds s2);
sds sdscpylen(sds s, const char *c, int len);
sds sdscpy(sds s, const char *c);
sds sdscatvsnprintf(sds s, char *fmt, va_list v);
sds sdscatsprintf(sds, char *fmt, ...);
sds sdsll2str(long long value);
sds sdsull2str(unsigned long long value);

//-------------------------sds tools------------------------------
int sdsavail(const sds s);
int sdslen(const sds s);
char* sdsstr(const sds s, size_t *strlen);

sds sdstrim(sds s, const char *trimset);
sds* sdssplitlen(sds s, const char *split, int splitlen, int *count);
sds sdsrange(sds s, int start, int end);
int sdsindexof(sds s, const char *c);
int sdscmp(sds s1, sds s2);
void sdstoupper(sds s);
void sdstolower(sds s);
sds sdsfromlonglong(long long l);
sds sdsjoin(char **argv, int argc, char *sep);
void sdsmapchars(sds s, const char *from, const char *to, int setlen);
sds sdsreplacen(sds s, const char *replace, int replacelen, int n);

sds sdsMakeRoomFor(sds s, int addlen);
void sdsincrlen(sds s, int len);

#endif //REDIS_SDS_H
