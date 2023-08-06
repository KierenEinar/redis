//
// Created by kieren jiang on 2023/7/25.
//

#ifndef REDIS_QUICKLIST_H
#define REDIS_QUICKLIST_H

#include <stdlib.h>
#include <memory.h>
#include <stdio.h>

#include "ziplist.h"
#include "zmalloc.h"

#define QUICK_LIST_HEAD 0
#define QUICK_LIST_TAIL 1

#define QUICK_LIST_ENCODING_RAW 1
#define QUCIK_LIST_ENCODING_LZF 2

#define QUICK_LIST_INSERT_BEFORE 0
#define QUICK_LIST_INSERT_AFTER 1

#define AL_LIST_FORWARD 0
#define AL_LIST_BACKWARD 1

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
    int idx;
    unsigned int size;
    long long llvalue;
}quicklistEntry;

typedef struct quicklistIter{
    quicklist *ql;
    quicklistNode *current;
    unsigned char *zi;
    int offset;
    int direction;
}quicklistIter;

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

void quicklistRelease(quicklist *ql);

// check quicklist is empty.
int quicklistEmpty(quicklist *ql);

// get the entry of quicklist index.
int quicklistIndex(quicklist *ql, long long idx, quicklistEntry *entry);

// insert after entry.
void quicklistInsertBefore(quicklist *ql, quicklistEntry *entry, void *data, unsigned int size);

// insert before entry.
void quicklistInsertAfter(quicklist *ql, quicklistEntry *entry, void *data, unsigned int size);

// quicklistDeleteNode delete the node from quicklist.
void quicklistDeleteNode(quicklist *ql, quicklistNode *node);

// quicklistDelRange delete the range of start, count
int quicklistDelRange(quicklist *ql, const long start, const long count);

// quicklistPopCustom pop from head or tail.
// saver enable caller copy the _data into data.
int quicklistPopCustom(quicklist *ql, int where, void **data, unsigned int *size, long long *value,
                       void *(*saver)(void *data, unsigned int size));

void *quicklistSaver(void *data, unsigned int size);

// quicklistCreateIterator create a new iter.
quicklistIter *quicklistCreateIterator(quicklist *ql, int direction);

// iter the next entry.
int quicklistNext(quicklistIter *iter, quicklistEntry *entry);

// delete the entry while iterating.
void quicklistDelEntry(quicklistIter *iter, quicklistEntry *entry);

// release the iter.
void quicklistReleaseIter(quicklistIter *iter);

void quicklistTest();

#endif //REDIS_QUICKLIST_H
