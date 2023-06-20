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
        return dictExpand(d, d->ht[0].used * 2);
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

static void _dictRehashStep(dict *d) {
    if (d->iterators == 0) dictRehash(d, 1);
}

static void _dictSetKey(dict *d, dictEntry *entry, void *key) {
    if (d->dictType->dupKey) {
        entry->key= d->dictType->dupKey(key);
    } else {
        entry->key = key;
    }
}


static void _dictSetVal(dict *d, dictEntry *entry, void *val) {
    if (d->dictType->dupValue) {
        entry->value.ptr = d->dictType->dupValue(val);
    } else {
        entry->value.ptr = val;
    }
}

static void _dictSetSignedInteger(dictEntry *entry, int64_t val) {
    entry->value.s64 = val;
}

static void _dictSetUnSignedInteger(dictEntry *entry, uint64_t val) {
    entry->value.u64 = val;
}

static void _dictFreeVal(dict *d, dictEntry *entry) {
    if (d->dictType->freeValue) {
        d->dictType->freeValue(entry->value.ptr);
    }
}

static void _dictFreeKey(dict *d, dictEntry *entry) {
    if (d->dictType->freeKey) {
        d->dictType->freeKey(entry->key);
    }
}

static dictEntry* _dictGenericDelete(dict *d, void *key, int nofree) {

    dictEntry *de;
    int table;
    uint64_t hash = dictKeyHash(d, key);
    unsigned long idx;
    for (table = 0; table <=1; table++) {

        dictEntry *pre = NULL;
        idx = hash & d->ht[table].mask;
        dictEntry *de = d->ht[table].table[idx];

        while (de) {

            if (d->dictType->hashFunction(key) == de->hash || key == de->key) {

                if (pre)
                    pre->next = de->next;
                else
                    d->ht[table].table[idx]->next = de->next;

                if (!nofree) {
                    _dictFreeKey(d, de);
                    _dictFreeVal(d, de);
                    zfree(de);
                }

                d->ht[table].used--;

                return de;
            }

            pre = de;
            de = de->next;
        }

        if (!dictIsRehashing(d)) break;

    }

    return NULL;
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

void dictReplace(dict *d, void *key, void *value) {

    dictEntry *existing, *entry, aux;

    entry = dictAddRow(d, key, &existing);
    if (entry) {
        _dictSetVal(d, entry, value);
        return;
    }

    aux = *existing;
    _dictSetVal(d, entry, value);
    _dictFreeVal(d, &aux);

}

dictEntry* dictFind(dict *d, const void *key) {

    dictht *ht;
    int i;
    uint64_t hash;
    unsigned long bucket;
    dictEntry *de;

    if (dictIsRehashing(d)) _dictRehashStep(d);

    hash = dictKeyHash(d, key);

    for (i=0; i<=1; i++) {
        ht = &d->ht[i];
        bucket = hash & ht->mask;
        de = ht->table[bucket];
        while (de) {
            if (de->hash == hash || de->key == key) {
                return de;
            }
            de = de->next;
        }
        if (!dictIsRehashing(d)) break;
    }

    return NULL;
}

void* dictFetchValue(dict *d, const void *key) {

    dictEntry *de = dictFind(d, key);
    if (!de) return NULL;
    return de->value.ptr;
}


dictEntry* dictAddRow(dict *d, void *key, dictEntry** existing) {

    dictht *n;
    if (dictIsRehashing(d)) _dictRehashStep(d);
    uint64_t keyhash = dictKeyHash(d, key);
    long idx = _dictKeyIndex(d, keyhash, key, existing);
    if (idx == -1) return NULL;
    dictEntry *de = zmalloc(sizeof(*de));
    _dictSetKey(d, de, key);
    de->hash = keyhash;
    n = &d->ht[0];
    if (dictIsRehashing(d))
        n = &d->ht[1];

    n->used++;
    de->next = n->table[idx];
    return de;
}

int dictRehash(dict *d, int n) {

    int empty_visit = n * 10;
    if (!dictIsRehashing(d)) return 0;

    dictEntry *de;
    unsigned long bucket;
    dictht ht = d->ht[0];

    while (n-- && ht.used != 0) {

        // we are sure there is elements en dict because ht.used != 0
        while ((de = (ht.table[d->rehash_idx])) == NULL && empty_visit) {
            d->rehash_idx++;
            if (--empty_visit == 0) return 1;
        }

        dictEntry *next;

        while (de) {
            next = de->next;
            bucket = de->hash & d->ht[1].mask;
            de->next = d->ht[1].table[bucket];
            d->ht[1].table[bucket] = de;
            de = next;
            d->ht[0].used--;
            d->ht[1].used++;
        }

        d->ht[0].table[d->rehash_idx++] = NULL;

        if (d->ht[0].used == 0) {
            zfree(d->ht[0].table);
            _dictReset(&d->ht[0]);
            d->ht[0] = d->ht[1];
            _dictReset(&d->ht[0]);
            d->rehash_idx = -1;
            return 0;
        }

    }

    return 1;

}

int dictDelete(dict *d, void *key) {
    return _dictGenericDelete(d, key, 0) ? DICT_OK : DICT_ERR;
}

dictEntry* dictUnlink(dict *d, void *key) {
    return _dictGenericDelete(d, key, 1);
}

void freeUnlinkEntry(dict *d, dictEntry* de) {
    _dictFreeKey(d, de);
    _dictFreeVal(d, de);
    zfree(de);
}

void dictSafeGetIterator(dict *d, dictIter *di) {

}

void dictGetIterator(dict *d, dictIter *di) {


}
