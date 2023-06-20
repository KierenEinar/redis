//
// Created by kieren jiang on 2023/6/18.
//

#ifndef REDIS_DICT_H
#define REDIS_DICT_H

#include <stdint.h>
#include <stdlib.h>
#include <limits.h>
#include <memory.h>
#include "zmalloc.h"
#include "crc16.h"
#define DICT_OK 0
#define DICT_ERR -1

#define dictIsRehashing(d) (((d)->rehash_idx) > 0)
#define dictKeyHash(d, k) (((d)->dictType)->hashFunction(k))
#define HT_MIN_SIZE 16
typedef struct dictType {
    uint64_t (*hashFunction) (void *key); // hash function
    int (*compare) (void *key, void *ptr); // compare dictEntry
    void* (*dupKey) (void *key);
    void* (*dupValue) (void *value);
    void (*freeKey) (void *key);
    void (*freeValue) (void *value);
}dictType;

typedef struct dictEntry {
    uint64_t hash;
    void *key;
    union {
        void *ptr;
        int64_t s64;
        uint64_t u64;
        double d;
    } value;
    struct dictEntry *next;

}dictEntry;

typedef struct dictht {
    dictEntry **table;
    unsigned long used;
    unsigned long size;
    unsigned long mask;
}dictht;

typedef struct dict {
    dictType *dictType;
    dictht ht[2];
    long rehash_idx;
    long iterators;
} dict;

typedef struct dictIter {
    dict *d;
    dictEntry *iter;
    dictEntry *nextIter;
    int index;
    int table;
    int safe;
    uint16_t fingerPrint;
}dictIter;

// ---------------------- public functions -----------------------

// create the dict.
dict* dictCreate(dictType *dictType);

// if dictEntry exists, return null and fill *existing if the ptr non null.
// otherwise return the entry which added to the slots.
dictEntry* dictAddRow(dict *d, void *key, dictEntry** existing);

// add entry to the dict, return DICT_OK if success, otherwise DICT_ERR.
int dictAdd(dict *d, void *key, void *value);

// replace the entry if key exists.
void dictReplace(dict *d, void *key, void *value);

// delete the entry from the dict and free it.
int dictDelete(dict *d, void *key);
// delete the entry from the dict but not free it, it's responsible caller call the freeUnlinkEntry free entry.
dictEntry* dictUnlink(dict *d, void *key);

void freeUnlinkEntry(dict *d, dictEntry* de);
// clear the dict, but not release the space.
void dictEmpty(dict *d, void (callback)(const void*));
// clear the dict, and free the space allocated from heap.
void dictRelease(dict *d);

// find the entry which key exists from the table, if not exists return null.
dictEntry* dictFind(dict *d, const void *key);
// fetch the value which key exists from the table, return the ptr persist int the table.
void* dictFetchValue(dict *d, const void *key);
// fetch the value which key exists from the table, return the us persist int the table.
//uint64_t dictGetUnsignedInteger(dict *d, const void *key);
//// fetch the value which key exists from the table, return the s persist int the table.
//int64_t dictGetSignedInteger(dict *d, const void *key);
//// fetch the value which key exists from the table, return the double persist int the table.
//double dictFetchDouble(dict *d, const void *key);

// dict resize, if the dict is rehashing or the size eq than the d[0].size, DICT_ERR return.
// otherwise rehash_idx set to 0 and, d[1]->table is created, DICT_OK return.
int dictResize(dict *d);

// dict expand the size.
int dictExpand(dict *d, unsigned long size);

// dict rehash by n step, if done 0 return, more to rehash 1 return.
int dictRehash(dict *d, int step);

// enable dict rehash.
void dictEnableResize(void);
// disable dict rehash.
void dictDisableResize(void);

// create the dict iterator
void dictSafeGetIterator(dict *d, dictIter *di);
// create the unsafe iterator
void dictGetIterator(dict *d, dictIter *di);
// get the next dict entry
dictEntry* dictNext(dictIter *di);
// release iter
void dictReleaseIter(dictIter *di);

// scan the dict
unsigned long dictScan(dict *d, unsigned long cursor, void (*dictScanFunction)(dictEntry *de));


#endif //REDIS_DICT_H
