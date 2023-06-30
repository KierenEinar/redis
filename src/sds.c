//
// Created by kieren jiang on 2023/5/28.
//

#include <memory.h>

#include "sds.h"
#include "zmalloc.h"

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
sds sdsMakeRoomFor(sds s, size_t len) {
    return NULL;
}
sds sdscatlen(const char *c, size_t len) {
    return NULL;
}
sds sdscatsds(sds s) {
    return NULL;
}
size_t sdslen(sds s) {
    return 0;
}
sds sdscatfmt(sds s, const char *fmt, ...) {
    return NULL;
}
sds sdscatprintf(sds s, const char *fmt, ...) {
    return NULL;
}
sds sdscatvnprintf(sds s, const char *fmt, va_list list) {
    return NULL;
}