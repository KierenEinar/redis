//
// Created by kieren jiang on 2023/2/20.
//

#ifndef REDIS_DICT_H
#define REDIS_DICT_H

#define DICT_OK 1
#define DICT_ERR 0

#define DICT_HT_INITIAL_BUCKETS_BIT 4
#define DICT_HT_FORCE_RESIZE_RATIO 5

#define DICT_HT_HASH_COMPARER_BITS 8

#include <stdint.h>

typedef struct dictType {
    uint64_t (*hash)(const void *key);
    void* (*keyDup) (const void *key);
    void* (*valDup) (const void *value);
    int (*keyComparer)(const void *key1, const void *key2);
    void  (*keyDestructor)(const void *key);
    void  (*valDestructor)(const void *value);
}dictType;

typedef struct dictEntry{
    unsigned char topHash;
    void *key;
    union {
        void *value;
        uint64_t u_64;
        int64_t s_64;
        double d;
    } v;
    struct dictEntry *nextEntry;
}dictEntry;

typedef struct dictht{
    dictEntry **tables;
    unsigned long size;
    unsigned long used;
    unsigned long mask;
}dictht;

typedef struct dict {
    dictht ht[2];
    dictType *dictType;
    int iterators;
    int rehashIdx;
    unsigned long hash0; // for rand hash function
} dict;

//------------API-------------
dict* dictCreate(dictType *type);
int dictAdd(dict *d, void *key, void *val);
dictEntry *dictAddRow(dict *d, void *key, dictEntry **existing);
int dictRehash(dict *d, int n);
int dictReplace(dict *d, void *key, void *val);
dictEntry* dictFind(dict *d, const void *key);
void* dictFetchValue(dict *d, const void *key);
uint64_t dictFetchUnsignedInteger(dict *d, void *key);
int64_t dictFetchSignedInteger(dict *d, void *key);
int dictSetUnsignedInteger(dict *d, void *key, uint64_t us);
int dictSetSignedInteger(dict *d, void *key, int64_t s);
int dictDelete(dict *d, const void *key);
int dictExpand(dict *d, unsigned long size);
int dictShrink(dict *d);
int dictRelease(dict *d);
void enableDictResize();
void disableDictResize();
int dictRehashMillSeconds(dict *d, unsigned long ms);
dictEntry *dictGetRandomKey(dict *d);
int dictGetSomeKeys(dict *d, dictEntry **de, int count);
//-----------private prototype-------



#endif //REDIS_DICT_H
