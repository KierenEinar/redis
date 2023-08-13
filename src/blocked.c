//
// Created by kieren jiang on 2023/8/13.
//

#include "server.h"

void blockForKeys(client *c, robj **argv, int argc, long long timeout) {

    robj *key;
    list *l;

    while (argc) {

        key = *argv;

        if (dictFind(c->bpop.blocking_keys, key) != NULL)
            continue;

        incrRefCount(key);

        dictAdd(c->bpop.blocking_keys, key, NULL);

        if (dictFind(c->db->blocking_keys, key) == NULL) {
            incrRefCount(key);
            l = listCreate();
            dictAdd(c->db->blocking_keys, key, l);
        } else {
            l = dictFetchValue(c->db->blocking_keys, key);
        }

        listAddNodeTail(l, c);

        (*argv)++;
        argc--;
    }

    c->bpop.timeout = timeout;
    c->flag &= CLIENT_BLOCKED;
}
