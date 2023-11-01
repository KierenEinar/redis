//
// Created by kieren jiang on 2023/5/28.
//

#ifndef REDIS_SDS_H
#define REDIS_SDS_H
#include <stdlib.h>
#include <stdarg.h>

#define SDS_PREALLOC (1024 * 1024)

typedef char* sds;

typedef struct sdshdr {
    size_t  used;
    size_t  free;
    char    buf[];
} __attribute__((packed)) sdshdr ;


sds sdsnew(const char *c);
sds sdsnewlen(const char *c, size_t len);
sds sdsdup(sds s);
sds sdsempty();
sds sdsclear(sds s);
void sdsfree(sds s);
sds sdsMakeRoomFor(sds s, size_t len);
sds sdscatlen(sds dest, const char *c, size_t len);
sds sdscatsds(sds dest, sds src);
size_t sdslen(sds s);
void sdsincrlen(sds s, size_t len);
// get the free space of sds.
size_t sdsavail(sds s);

// sds cat format string
// this function only handles incompatible subset of printf-alike format specifiers,
// %s - C string
// %S - sds string
// %i - signed int
// %I - signed int 64bit, like (int64_t, long long)
// %u - unsigned int
// %U - unsigned int 64bit, like (unsigned long long, uint64_t)
sds sdscatfmt(sds s, const char *fmt, ...);
sds sdscatprintf(sds s, const char *fmt, ...);
sds sdscatvnprintf(sds s, const char *fmt, va_list list);
sds sdsrange(sds s, long start, long end);
size_t sdsmove(sds s, long pos, size_t len);
#endif //REDIS_SDS_H


