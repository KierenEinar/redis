//
// Created by kieren jiang on 2023/7/25.
//

#ifndef REDIS_QUICKLIST_H
#define REDIS_QUICKLIST_H

#include <stdlib.h>

#include "ziplist.h"
#include "zmalloc.h"

#define QUICK_LIST_HEAD 0
#define QUICK_LIST_TAIL 1

#define QUICK_LIST_ENCODING_RAW 1
#define QUCIK_LIST_ENCODING_LZF 2

#define QUICK_LIST_INSERT_BEFORE 0
#define QUICK_LIST_INSERT_AFTER 1

typedef struct quicklistNode{
    struct quicklistNode *prev;
    struct quicklistNode *next;
    unsigned char *zl;
    unsigned int size;
    unsigned int count:16;
    unsigned int encoding:2; // raw=1, compress=2
}quicklistNode;


typedef struct quicklist{
    quicklistNode *head;
    quicklistNode *tail;
    unsigned int count; // ziplist entries count
    unsigned int len; // nodes count
    int fill; // fill between [-1, -5], ziplist size limit [4k,8k,16k,32k,64k], gt 0, ziplist entries count eq fill. set default -2(8k)
}quicklist;

typedef struct quicklistEntry{
    quicklist *ql;
    quicklistNode *node;
    unsigned char *zlentry;
    unsigned char *str;
    unsigned int idx;
    unsigned int size;
    long long llvalue;
}quicklistEntry;

// create a new quicklist .
quicklist *quicklistCreate(void);

// create a new quicklist by options.
quicklist *quicklistNew(int fill);

// Add new entry to head node of quicklist.
//
// Returns 0 if used existing head.
// Returns 1 if new head created.
int quicklistPushHead(quicklist *ql, void *data, unsigned int size);

// Add new entry to tail node of quicklist.
//
// Returns 0 if used existing tail.
// Returns 1 if new tail created.
int quicklistPushTail(quicklist *ql, void *data, unsigned int size);

// a wraaper to push header or tail.
int quicklistPush(quicklist *ql, void *data, unsigned int size, int where);

// get the entry of quicklist index.
int quicklistIndex(quicklist *ql, const long long value, quicklistEntry *entry);

// insert after entry.
int quicklistInsertBefore(quicklist *ql, quicklistEntry *entry, void *data, unsigned int size);

// insert before entry.
int quicklistInsertAfter(quicklist *ql, quicklistEntry *entry, void *data, unsigned int size);

#endif //REDIS_QUICKLIST_H
