//
// Created by kieren jiang on 2023/5/28.
//

#ifndef REDIS_SDS_H
#define REDIS_SDS_H
#include <stdlib.h>
#include <stdarg.h>

typedef char* sds;

typedef struct sdshdr {
    size_t  used;
    size_t  free;
    char    buf[];
} __attribute__((packed)) sdshdr ;

sds sdsnewlen(const char *c, size_t len);
sds sdsdup(sds s);
sds sdsempty();
void sdsfree(sds s);
sds sdsMakeRoomFor(sds s, size_t len);
sds sdscatlen(const char *c, size_t len);
sds sdscatsds(sds s);
size_t sdslen(sds s);
sds sdscatfmt(sds s, const char *fmt, ...);
sds sdscatprintf(sds s, const char *fmt, ...);
sds sdscatvnprintf(sds s, const char *fmt, va_list list);

#endif //REDIS_SDS_H


