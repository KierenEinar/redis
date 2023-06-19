//
// Created by kieren jiang on 2023/6/18.
//

#include "dict.h"

// ------------ private function -------------

static int dict_can_resize = 1;

static int dict_force_resize_ratio = 5;

static void _dictReset(struct dictht* ht) {
    ht->used = 0;
    ht->size = 0;
    ht->mask = 0;
    ht->table = NULL;
}

static unsigned long _nextPowerOfTwo (unsigned long size) {

    unsigned long realsize = HT_MIN_SIZE;

    if (size >= LONG_MAX) {
        return LONG_MAX + 1UL;
    }

    while (1) {
        if (realsize>=size) {
            return realsize;
        }
        realsize*=2;
    }
}

static uint64_t dictEntryKeyHash(dict *d, dictEntry *de) {
    return d->dictType->hashFunction(de->key);
}

static int _dictExpandIfNeeded(dict *d) {

    if (dictIsRehashing(d)) return DICT_OK;

    if (d->ht[0].used == 0) return dictExpand(d, HT_MIN_SIZE);

    if (d->ht[0].used >= d->ht[0].size &&
        (dict_can_resize || d->ht[0].used / d->ht[0].size > dict_force_resize_ratio)) {
        return dictExpand(d, d->ht[0].used);
    }

    return DICT_OK;

}

static long _dictKeyIndex(dict *d, uint64_t keyhash, void *key, dictEntry **existing) {

    long idx;
    dictEntry *de;
    dictht ht;
    int i;

    if (_dictExpandIfNeeded(d) == DICT_ERR)
        return -1;

    if (existing) *existing = NULL;

    for (i=0; i<1; i++) {
        ht = d->ht[i];
        idx = keyhash&ht.mask;
        de = ht.table[idx];
        while (de) {
            if (dictEntryKeyHash(d, de) == keyhash || de->key == key) {
                if (existing) *existing = de;
                return -1;
            }
        }

        if (!dictIsRehashing(d))
            break;
    }

    return idx;


}

// ----------------- public function ---------------

dict* dictCreate(dictType *dictType) {
    dict *d = zmalloc(sizeof(*d));
    _dictReset(&d->ht[0]);
    _dictReset(&d->ht[1]);
    d->dictType = dictType;
    d->iterators = 0;
    d->rehash_idx = -1;
    return d;
}

int dictResize(dict *d) {

    unsigned long expand_size;

    if (!dict_can_resize || dictIsRehashing(d))
        return DICT_ERR;
    expand_size = HT_MIN_SIZE;

    if (d->ht[0].used > d->ht[0].size)
        expand_size = d->ht[0].used * 2;

    return dictExpand(d, expand_size);
}


void dictEnableResize(void) {
    dict_can_resize = 1;
}

// disable dict rehash.
void dictDisableResize(void) {
    dict_can_resize = 0;
}


int dictExpand(dict *d, unsigned long size) {

    if (dictIsRehashing(d) || d->ht[0].used > size)
        return DICT_ERR;

    unsigned long realsize = _nextPowerOfTwo(size);

    if (realsize == d->ht[0].size) return DICT_ERR;

    if (d->ht[0].table == NULL) {
        d->ht[0].size = realsize;
        d->ht[0].mask = realsize - 1;
        d->ht[0].table = zmalloc(sizeof(dictht*) * realsize);
        return DICT_OK;
    }

    d->ht[1].table = zmalloc(sizeof(dictht*) * realsize);
    d->ht[1].size = realsize;
    d->ht[1].mask = realsize - 1;
    d->rehash_idx = -1;

    return DICT_OK;
}

int dictAdd(dict *d, void *key, void *value) {

    dictEntry *de;

    if ((de = dictAddRow(d, key, NULL)) == NULL)
        return DICT_ERR;

    if (d->dictType->dupValue)
        de->value.ptr = d->dictType->dupValue(value);
    else
        de->value.ptr = value;
    return DICT_OK;
}


dictEntry* dictAddRow(dict *d, void *key, dictEntry** existing) {

    dictht *n;

    if (dictIsRehashing(d)) dictRehashStep(d, 1);

    uint64_t keyhash = dictKeyHash(d, key);

    long idx = _dictKeyIndex(d, keyhash, key, existing);

    if (idx == -1) return NULL;

    dictEntry *de = zmalloc(sizeof(*de));

    if (d->dictType->dupKey)
        de->key = d->dictType->dupKey(key);
    else
        de->key = key;

    de->hash = keyhash;

    n = &d->ht[0];

    if (dictIsRehashing(d))
        n = &d->ht[1];

    de->next = n->table[idx];
    return de;
}

int dictRehashStep(dict *d, int n) {
    return DICT_ERR;
}