//
// Created by kieren jiang on 2023/2/20.
//

#ifndef REDIS_DICT_H
#define REDIS_DICT_H

typedef struct dictType {
    unsigned int (*hash)(const void *key);
    int (*keyComparer)(const void *key1, const void *key2);
    void* (*keyDup) (const void *key);
    void  (*keyDestructor)(const void *key);
    void* (*valDup) (const void *value);
    void  (*valDestructor)(const void *value);
};

typedef struct dictEntry{
    void *key;
    void *value;
    dictEntry *nextEntry;
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
} dict;

//------------API-------------
dict* dictCreate(dictType *type);
int dictAdd(dict *d, void *key, void *val);
int dictReplace(dict *d, void *key, void *val);
dictEntry* dictFind(dict *d, void *key);
void* dictFetchValue(dict *d, void *key);
int dictDelete(dict *d, void *key);


#endif //REDIS_DICT_H
