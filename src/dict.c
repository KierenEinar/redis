//
// Created by kieren jiang on 2023/2/20.
//

#include "dict.h"
#include <limits.h>
#include <stdlib.h>
#include <sys/time.h>

static int dict_resize_enable = 1;

/*------------------ private protocol ------------------*/
static int _dictIsRehashing(dict *d);
static void _dictRehashStep(dict *d);
static int _keyIndex(dict *d, const void *key, unsigned long *topHash);
static void _dictInit(dict *d, dictType *type);
static int _dictExpandIfNeeded(dict *d);
static int _dictShrinkIfNeeded(dict *d);
static unsigned long _nextPowerOfTwo(unsigned long n);
static int _isPowerOfTwo(unsigned long n);
//static unsigned char _lowHash(unsigned long hash, uint8_t lowBits);
static unsigned char _topHash(unsigned long hash, uint8_t topBits);
static void _setEntryKey(dict *d, dictEntry *entry, void *key);
static void _setEntryValue(dict *d, dictEntry *entry, void *value);
static void _dictReset(dictht *ht);
static void _dictFreeKey(dict *d, void *key);
static void _dictFreeValue(dict *d, void *value);
static int  _dictResize(dict *d, unsigned long realSize);
static int _dictClear(dict *d, dictht *ht);
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
    dictEntry *entry = (dictEntry*)malloc(sizeof (*entry));
    entry->topHash = topHash;
    entry->nextEntry = d->ht[table].tables[idx];
    _setEntryKey(d, entry, key);
    _setEntryValue(d, entry, value);
    d->ht[table].tables[idx] = entry;
    d->ht[table].used++;
    return DICT_OK;
}

int dictRehash(dict *d, int n) {

    if (!_dictIsRehashing(d)) return DICT_OK;
    int emptyVisit = n * 10;
    dictEntry *de, *nextEntry;
    unsigned long deHash;
    int idx;
    while (n--) {

        if (!d->ht[0].used) {
            free(d->ht[0].tables);
            _dictReset(&d->ht[0]);
            d->ht[0] = d->ht[1];
            _dictReset(&d->ht[1]);
            d->rehashIdx = -1;
            break;
        }

        do {
            de = d->ht[0].tables[d->rehashIdx++];
        } while (de == NULL && (emptyVisit--) > 0);

        if (!de) return DICT_OK;

        while (de) {
            deHash = d->dictType->hash(de->key);
            nextEntry = de -> nextEntry;
            idx = deHash & d->ht[1].mask;
            de -> nextEntry = d->ht[1].tables[idx];
            d->ht[1].tables[idx] = de;
            de = nextEntry;
            d->ht[0].used--;
            d->ht[1].used++;
        }

        d->ht[0].tables[d->rehashIdx-1] = NULL;
    }

    return DICT_OK;

}

int dictReplace(dict *d, void *key, void *val) {

    if (_dictIsRehashing(d)) _dictRehashStep(d);

    dictEntry *de;
    int table, idx;
    unsigned long hash = d->dictType->hash(key);
    unsigned char topHash = _topHash(hash, DICT_HT_HASH_COMPARER_BITS);

    for (table=0; table<=1; table++) {
        idx = hash & d->ht[table].mask;
        de = d->ht[table].tables[idx];
        if (de) {
            if (de->topHash == topHash && (de->key == key || (d->dictType->keyComparer(key, de->key) == 0))) {
                _dictFreeValue(d, de->value);
                _setEntryValue(d, d->ht[table].tables[idx], val);
                return DICT_OK;
            }
            de = de->nextEntry;
        }
        if (!_dictIsRehashing(d)) break;
    }

    return DICT_ERR;
}

dictEntry* dictFind(dict *d, const void *key) {

    int idx;
    unsigned long topHash;
    unsigned char idxTopHash;
    dictEntry *de;
    if (_dictIsRehashing(d)) _dictRehashStep(d);
    unsigned long hash = d->dictType->hash(key);
    topHash = _topHash(hash, DICT_HT_HASH_COMPARER_BITS);

    for (int table=0; table<=1; table++) {
        idx = hash & d->ht[table].mask;
        de = d->ht[table].tables[idx];
        while (de) {
            idxTopHash = de->topHash;
            if (idxTopHash == topHash && (de->key == key || d->dictType->keyComparer(key, de->key) == 0)) {
                return de;
            }
            de = de->nextEntry;
        }
        if (!_dictIsRehashing(d)) break;
    }

    return NULL;
}

void* dictFetchValue(dict *d, const void *key) {
    dictEntry *entry = dictFind(d, key);
    return (entry) ? entry->value : NULL;
}

int dictDelete(dict *d, const void *key) {

    if (_dictIsRehashing(d)) _dictRehashStep(d);

    unsigned int table;
    dictEntry **de;
    unsigned long hash = d->dictType->hash(key);
    unsigned char topHash = _topHash(hash, DICT_HT_HASH_COMPARER_BITS);

    for (table=0; table<=1; table++) {
        int idx = hash & d->ht[table].mask;
        de = &d->ht[table].tables[idx];
        if ((*de) != NULL) {
            if ((*de)->topHash == topHash && ((*de)->key == key || d->dictType->keyComparer((*de)->key, key) == 0)) {
                dictEntry *ptr = *de;
                *de = ptr->nextEntry;
                _dictFreeKey(d, ptr->key);
                _dictFreeValue(d, ptr->value);
                free(ptr);
                d->ht[table].used--;
                return DICT_OK;
            }
            de = &((*de)->nextEntry);
        }

        if (!_dictIsRehashing(d)) break;
    }

    return DICT_ERR;

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

int dictExpand(dict *d, unsigned long size) {
    if (_dictIsRehashing(d) || d->ht[0].size > size) return DICT_ERR;

    unsigned long realSize = _nextPowerOfTwo(size);
    return _dictResize(d, realSize);
}

int dictShrink(dict *d) {
    if (_dictIsRehashing(d) || d->ht[0].size <= (1 << DICT_HT_INITIAL_BUCKETS_BIT)) return DICT_ERR;

    unsigned long realSize = d->ht[0].size >> 1;
    return _dictResize(d, realSize);

}

int dictRelease(dict *d) {
    unsigned long h0 = _dictClear(d, &d->ht[0]);
    unsigned long h1 =_dictClear(d, &d->ht[1]);
    free(d);
    return h0 + h1;
}

void enableDictResize() {
    dict_resize_enable = 1;
}
void disableDictResize() {
    dict_resize_enable = 0;
}

long long timeInMillSeconds(void) {
    struct timeval tv;
    gettimeofday(&tv, NULL);
    return (long long) (tv.tv_sec * 1000 + tv.tv_usec/1000);
}

int dictRehashMillSeconds(dict *d, unsigned long ms) {
    long long start = timeInMillSeconds();
    long long end;
    int rehashed = 0;
    while (1) {
        dictRehash(d,100);
        rehashed+=100;
        end = timeInMillSeconds();
        if (end - start >= ms) {
            break;
        }
    }
    return rehashed;
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
    dictRehash(d, 1);
}

static int _dictExpandIfNeeded(dict *d) {

    if (_dictIsRehashing(d)) return DICT_OK;

    // first init
    if (d->ht[0].size == 0) return dictExpand(d, 1 << DICT_HT_INITIAL_BUCKETS_BIT);

    // in this two case, we choose to expand,
    // used >= size && dict_expand_enable
    // or used/size reach ratio
    if (( d->ht[0].used >= d->ht[0].size && dict_resize_enable ) ||
            d->ht[0].used / d->ht[0].size >= DICT_HT_FORCE_RESIZE_RATIO) {
        return dictExpand(d, d->ht[0].used*2);
    }

    return DICT_OK;
}

static int _dictShrinkIfNeeded(dict *d) {
    if (_dictIsRehashing(d)) return DICT_OK;

    if (( d->ht[0].used < d->ht[0].size && dict_resize_enable ) ||
            d->ht[0].size / d->ht[0].used  >= DICT_HT_FORCE_RESIZE_RATIO) {
        return dictShrink(d);
    }

    return DICT_OK;

}

static int _dictResize(dict *d, unsigned long realSize) {

    if (realSize < DICT_HT_INITIAL_BUCKETS_BIT) return DICT_ERR;
    dictht n;
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

static int _keyIndex(dict *d, const void *key, unsigned long *topHash) {

    unsigned int table, idx, idxTopHash = 0;
    unsigned long hash;
    dictEntry *de;

    if (!_dictExpandIfNeeded(d)) {
        return -1;
    }

    hash = d->dictType->hash(key);
    *topHash = _topHash(hash, DICT_HT_HASH_COMPARER_BITS);
    // lowHash = _lowHash(hash, DICT_HT_HASH_COMPARER_BITS);

    for (table=0; table<=1; table++) {
        idx = hash & d->ht[table].mask;
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

static int _isPowerOfTwo(unsigned long n) {

    int i = 0;
    while (i<64) {
        if (n == 1<<(i++)) {
            return 1;
        }
    }
    return 0;
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
    if (d->dictType->keyDup) {
        entry->key = d->dictType->keyDup(key);
        return;
    }
    entry->key = key;
}

static void _setEntryValue(dict *d, dictEntry *entry, void *value) {
    if (d->dictType->valDup) {
        entry->value = d->dictType->valDup(value);
        return;
    }
    entry->value = value;
}

static void _dictFreeKey(dict *d, void *key) {
    if (d->dictType->keyDestructor) {
        d->dictType->keyDestructor(key);
    }
}

static void _dictFreeValue(dict *d, void *value) {
    if (d->dictType->valDestructor) {
        d->dictType->valDestructor(value);
    }
}

static int _dictClear(dict *d, dictht *ht) {

    unsigned long processed, idx;

    dictEntry *de, *ptr;

    for (idx=0; idx<ht->size; idx++) {
        de = ht->tables[idx];
        while (de) {
            _dictFreeKey(d, de->key);
            _dictFreeValue(d, de->value);
            ptr = de;
            de = ptr->nextEntry;
            free(ptr);
            processed++;
        }
    }

    free(ht->tables);
    _dictReset(ht);
    return processed;
}
