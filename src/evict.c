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
    return mstime() / CLOCK_RESOLUTION & CLOCK_MAX;
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

int freeMemoryIfNeeded(void) {

    unsigned long long mem_reported, mem_tofree, mem_freed;

    mem_reported = zmalloc_used_memory();

    if (mem_reported < server.maxmemorry) {
        return C_OK;
    }

    if (server.maxmemorry_policy == MAXMEMORY_NOEVICTION) {
        goto cantfree;
    }

    mem_freed = 0;
    mem_tofree = mem_reported - server.maxmemorry;

    while (mem_freed < mem_tofree) {

        int bestdbid, dbid;
        sds bestkey;
        dict *d;
        db *db;
        dictEntry *de;
        struct evictionPoolEntry *pool = EvictionPoolLRU;
        static unsigned long long next_db;

        if (server.maxmemorry_policy & (MAXMEMORY_FLAG_LRU|MAXMEMORY_FLAG_LFU) || server.maxmemorry_policy == MAXMEMORY_VOLATILE_TTL) {

            while (bestkey == NULL) {

                for (dbid=0; dbid<server.db_nums; dbid++) {
                    db = server.db + dbid;
                    d = db->dict;
                    if (!(server.maxmemorry_policy & MAXMEMORY_ALLKEYS))
                        d = db->expires;
                    if (!dictSize(d)) continue;
                    evictionPoolPopulate(dbid, d, db->dict, pool);
                }

                for (int k=EVICTION_POOL_SIZE-1; k>=0; k--) {
                    if (pool[k].key == NULL) continue;

                    sds key = pool[k].key;

                    if (server.maxmemorry_policy & MAXMEMORY_ALLKEYS) {
                        de = dictFind(db->dict, key);
                    } else {
                        de = dictFind(db->expires, key);
                    }

                    pool[k].key = NULL;
                    if (pool[k].key != pool[k].cached)
                        sdsfree(pool[k].key);
                    pool[k].idle = 0;
                    pool[k].dbid = 0;

                    if (de) {
                        bestdbid = pool[k].dbid;
                        bestkey = de->key;
                        break;
                    } else {
                        // continue iterate
                    }
                }
            }


        } else if (server.maxmemorry_policy == MAXMEMORY_ALLKEYS_RANDOM || server.maxmemorry_policy == MAXMEMORY_VOLATILE_RANDOM) {

            for (dbid=0; dbid <server.db_nums; dbid++) {
                int next_db = (++next_db) % server.db_nums;
                db = server.db + next_db;
                d = server.maxmemorry_policy & MAXMEMORY_ALLKEYS ? db->dict : db->expires;
                if (!dictSize(d)) continue;

                if (de) {
                    bestkey = de->key;
                    bestdbid = dbid;
                }
            }
        }


        if (bestkey) {
            robj *keyobj = createStringObject(bestkey, sdslen(bestkey));
            dbSyncDelete(db+bestdbid, keyobj);
            unsigned long long delta = mem_reported - zmalloc_used_memory();
            mem_freed+=delta;
            mem_tofree-=delta;

            decrRefCount(keyobj);

        }
    }


cantfree:
    //todo
}


