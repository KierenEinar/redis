//
// Created by kieren jiang on 2023/3/27.
//

#include "server.h"


#define EVICTION_POOL_SIZE 16
#define EVICTION_SDS_CACHE_SIZE 255
typedef struct evictionPoolEntry {
    int dbid;
    sds key;
    sds cached;
    unsigned long long idle;
};

static struct evictionPoolEntry *EvictionPoolLRU;

void evictionPoolAlloc(void) {
    struct evictionPoolEntry *ep;
    ep = zmalloc(sizeof(*ep) * EVICTION_POOL_SIZE);
    for (int j=0; j<EVICTION_POOL_SIZE; j++) {
        ep[j].key = NULL;
        ep[j].cached = sdsnewlen(NULL, EVICTION_SDS_CACHE_SIZE);
        ep[j].dbid = 0;
        ep[j].idle = 0;
    }
    EvictionPoolLRU = ep;
}

// return the lru_clock unix timestamp
unsigned int getLRUClock() {

    // todo add cache
    return ms_now() / CLOCK_RESOLUTION & CLOCK_MAX;
}

long long estimateIdleTime(robj *o) {

    unsigned int lruClock = getLRUClock();
    if (lruClock >= o->lru) {
        return lruClock - o->lru;
    } else {
        return CLOCK_MAX + lruClock - o->lru;
    }

}

void evictionPoolPopulate(int dbid, dict* sampleDict, dict* keyDict, struct evictionPoolEntry* pool) {

    dictEntry *sampleDictEntry[server.maxmemorry];

    int count = dictGetSomeKeys(sampleDict, sampleDictEntry, server.maxmemorry);

    while (count) {

        dictEntry *de;
        robj *o;
        long long idle;

        if (server.maxmemorry_policy & (MAXMEMORY_FLAG_LFU || MAXMEMORY_FLAG_LRU)) {
            if (sampleDict != keyDict) de = dictFind(keyDict, de->key);
            o = de->v.value;
        }

        if (server.maxmemorry & MAXMEMORY_FLAG_LRU) {
            idle = estimateIdleTime(o);
        } else if (server.maxmemorry & MAXMEMORY_FLAG_LRU) {
            // todo add lfu policy
        } else if (server.maxmemorry == MAXMEMORY_VOLATILE_TTL) {
            idle = ULLONG_MAX - de->v.s_64;
        } else {
            // todo panic
        }

        int k = 0;
        while (pool[k].key != NULL && pool[k].idle < idle) k++;

        // pool is full and the key is better than the worst
        if (k == 0 && pool[EVICTION_POOL_SIZE].key != NULL ) {
            continue;
        } else  if (k < EVICTION_POOL_SIZE && pool[k].key == NULL) {
            // do nothing
        } else {

            // position in middle

            // pool not full case
            if (pool[EVICTION_POOL_SIZE].key == NULL) {
                sds cached = pool[EVICTION_POOL_SIZE].cached;
                memmove(pool+k+1, pool+k, sizeof(pool[0]) * (EVICTION_POOL_SIZE - k - 1));
                pool[k].cached = cached;
            } else {
                sds cached = pool[0].cached;
                if (pool[0].key != cached) sdsfree(pool[0].key);
                memmove(pool+k, pool+k-1, sizeof(pool[0]) * (k-1));
                pool[k].cached = cached;
            }
        }


        int slen = sdslen(de->key);
        if (slen > EVICTION_SDS_CACHE_SIZE) {
            pool[k].key = sdsdup(de->key);
        } else {
            memcpy(pool[k].cached, de->key, slen+1);
            sdssetlen(pool[k].cached, slen);
        }

        pool[k].idle = idle;
        pool[k].dbid = dbid;

        count--;
    }


}



