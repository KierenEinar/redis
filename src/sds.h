//
// Created by kieren jiang on 2023/2/28.
//

#ifndef REDIS_SDS_H
#define REDIS_SDS_H

typedef char* sds;

struct __attribute__((__packed__)) sdshdr {
    int len;
    int free;
    char []buf;
};

//---------------------------------API--------------------------------

sds sdsnewlen(const char *c, int len);
sds sdsnew(const char *c);
sds sdsdup(const sds s);
sds sdsempty(void);
void sdsfree(sds s);
void sdsclear(sds s);
sds sdscatlen(sds s, const char *c, int len);
sds sdscat(sds s1, const char *c);
sds sdscatsds(sds s1, const sds s2);
void sdscpylen(sds s, const char *c, int len);
void sdscpy(sds s, const char *c);

//-------------------------sds tools------------------------------
int sdsavail(const sds s);
int sdslen(const sds s);

sds sdstrim(sds s, const char *trimset);
sds* sdssplitlen(const char* c1, int clen, const char *split, int splitlen, int *count);
sds sdsrange(sds s, int start, int end);
int sdsindexof(sds s, const char *c);
int sdscmp(sds s1, sds s2);
void sdstoupper(sds s);
void sdstolower(sds s);
sds sdsfromlonglong(long long l);
sds sdsjoin(char **argv, int argc, char *sep);
sds sdsmapchars(sds s, const char *from, const char *to, int setlen);


void sdsMakeRoomFor(sds s, int addlen);
void sdsincrlen(sds s, int len);

#endif //REDIS_SDS_H
