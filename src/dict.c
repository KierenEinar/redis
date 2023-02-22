//
// Created by kieren jiang on 2023/2/20.
//

#include "dict.h"
#include <limits.h>
#include <stdlib.h>

static int dict_expand_enable = 1;

/*------------------ private protocol ------------------*/
static int _dictIsRehashing(dict *d);
static void _dictRehashStep(dict *d);
static int _keyIndex(dict *d, const void *key, unsigned long *topHash);
static void _dictInit(dict *d, dictType *type);
static int _dictExpandIfNeeded(dict *d);
static unsigned long _nextPowerOfTwo(unsigned long n);
static unsigned char _lowHash(unsigned long hash, uint8_t lowBits);
static unsigned char _topHash(unsigned long hash, uint8_t topBits);
static void _setEntryKey(dict *d, dictEntry *entry, void *key);
static void _setEntryValue(dict *d, dictEntry *entry, void *value);

/*----------------- API implements ---------------------*/
dict* dictCreate(dictType *type){
    dict *d = malloc(sizeof(dict));
    _dictInit(d, type);
    return d;
}


int dictAdd(dict *d, void *key, void *value) {
    int idx;
    unsigned long topHash;
    if (_dictIsRehashing(d)) _dictRehashStep(d);
    idx = _keyIndex(d, key, &topHash);
    if (idx == -1) { // -1 means key exists
        return DICT_ERR;
    }

    int table = _dictIsRehashing(d) ? 1 : 0;
    dictEntry *entry = malloc(sizeof (*entry));
    entry->key = k;
    entry->value = value;
    entry->topHash = topHash;
    entry->nextEntry = d->ht[table];
    d->ht[table] = entry;
    d->ht[table].used++;
    _setEntryKey(d, entry, key);
    _setEntryKey(d, entry, value);
    return DICT_OK;
}

dictEntry* dictFind(dict *d, void *key) {




}


int dictExpand(dict *d, unsigned long size) {

    if (_dictIsRehashing(d) || d->ht[0].size > size) return DICT_ERR;

    dictht n;
    unsigned long realSize = _nextPowerOfTwo(size);
    n.tables = calloc(realSize, sizeof(dictEntry*));
    n.mask = realSize - 1;
    n.used = 0;
    n.size = realSize;
    if (d->ht[0].tables == NULL) {
        d->ht[0] = n;
        return DICT_OK;
    }

    d->ht[1] = n;
    d->rehashIdx = 0;
    return DICT_OK;
}

void enableDictExpand() {
    dict_expand_enable = 1;
}
void disableDictExpand() {
    dict_expand_enable = 0;
}


/*------------------ private functions -----------------*/
static void _dictReset(dictht *ht) {
    ht->tables = NULL;
    ht->size = 0;
    ht->mask = 0;
    ht->used = 0;
}

static int _dictIsRehashing(dict *d) {
    return d->rehashIdx != -1 ? 1 : 0;
}

static void _dictRehashStep(dict *d) {
    return;
}

int _dictExpandIfNeeded(dict *d) {

    if (_dictIsRehashing(d)) return DICT_OK;

    // first init
    if (d->ht[0].size == 0) return dictExpand(d, 1 << DICT_HT_INITIAL_BUCKETS_BIT);

    // in this two case, we choose to expand,
    // used >= size && dict_expand_enable
    // or used/size reach ratio
    if (( d->ht[0].used >= d->ht[0].size && dict_expand_enable ) ||
            d->ht[0].used / d->ht[0].size >= DICT_HT_FORCE_RESIZE_RATIO) {
        return dictExpand(d, d->ht[0].used*2);
    }

    return DICT_OK;
}

static int _keyIndex(dict *d, const void *key, unsigned long *topHash) {

    unsigned int table, idx, topHash, lowHash, idxTopHash = 0;
    unsigned long hash;
    dictEntry *de;

    if (!_dictExpandIfNeeded(d)) {
        return -1;
    }

    hash = d->dictType->hash(key);
    *topHash = _topHash(hash, DICT_HT_HASH_COMPARER_BITS);
    lowHash = _lowHash(hash, DICT_HT_HASH_COMPARER_BITS);

    for (table=0; table<=1; table++) {
        idx = lowHash & d->ht[table].mask;
        de = d->ht[table].tables[idx];
        while (de) {
            idxTopHash = de->topHash;
            if (idxTopHash == *topHash && d->dictType->keyComparer(key, de->key)) {
                return -1;
            }
            de = de->nextEntry;
        }
        if (!_dictIsRehashing(d)) break;
    }
    return idx;
}

static void _dictInit(dict *d, dictType *type) {
    _dictReset(&d->ht[0]);
    _dictReset(&d->ht[1]);
    d->dictType = type;
    d->iterators = 0;
    d->rehashIdx = -1;
}

unsigned long _nextPowerOfTwo(unsigned long n) {

    unsigned int i = DICT_HT_INITIAL_BUCKETS_BIT;
    unsigned long r = 1 << i;
    while (1) {
        if (r >= n || r == LONG_MAX) {
            return r;
        }
        r = 1 << (i++);
    }
}

static unsigned char _lowHash(unsigned long hash, uint8_t lowBits) {
    if (lowBits > 8) {
        lowBits = 8;
    }
    return hash & ((1 << lowBits) -1);
}

static unsigned char _topHash(unsigned long hash, uint8_t topBits) {
    if (topBits > 8) {
        topBits = 8;
    }
    int localBits = sizeof(long) == 4 ? 32 : 64;
    return hash >> (localBits - topBits) ;
}

static void _setEntryKey(dict *d, dictEntry *entry, void *key) {
    if (d->dictType->keyDup != NULL) {
        entry->key = d->dictType->keyDup(key);
        return;
    }
    entry->key = key;
}

static void _setEntryValue(dict *d, dictEntry *entry, void *value) {
    if (d->dictType->valDup != NULL) {
        entry->value = d->dictType->valDup(value);
        return;
    }
    entry->value = value;
}
