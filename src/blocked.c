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
    c->flag |= CLIENT_BLOCKED;
}

void unblockClient(client *c) {

    dictIter di;
    dictEntry *de;
    dictGetIterator(c->bpop.blocking_keys, &di);
    c->flag &= ~CLIENT_BLOCKED;
    while ((de = dictNext(&di)) != NULL) {

        robj *key = de->key;
        list *l = dictFetchValue(c->db->blocking_keys, key);
        if (l) {
            listNode *node = listSearchKey(l, c);
            if (node) {
                listDelNode(l, node);
                zfree(node);
            }
        }

        dictDelete(c->bpop.blocking_keys, key);

    }
}

int serveClientOnBlockedList(client *c, robj *key, robj *value) {

    addReplyMultiBulkLen(c, 2);
    addReplyBulk(c, key);
    addReplyBulk(c, value);

    return C_OK;
}

void handleClientsOnBlockedList(void) {

    list *l;
    listIter iter;
    readyList *rl;
    listNode *node;

    while (listLength(server.ready_keys)!= 0) {

        l = server.ready_keys;
        server.ready_keys = listCreate();
        listRewind(l, &iter);

        while ((node=listNext(&iter)) != NULL) {

            rl = node->value;
            dictDelete(rl->db->ready_keys, rl->key);

            robj *obj = lookupKeyWrite(rl->db, rl->key);
            if (obj != NULL && obj->type == REDIS_OBJECT_LIST) {

                list *clients = dictFetchValue(rl->db->blocking_keys, rl->key);

                while (listLength(clients) > 0) {

                    listNode *ln = listFirst(clients);
                    client *c = listNodeValue(ln);

                    int where = c->cmd->proc == blpopCommand ?
                            QUICK_LIST_HEAD : QUICK_LIST_TAIL;

                    robj *value = listTypePop(obj, where);
                    if (value) {
                        unblockClient(c);
                        if (serveClientOnBlockedList(c, rl->key, value) != C_OK) {
                            listTypePush(obj, value, where);
                        }
                        decrRefCount(value);
                    } else {
                        break;
                    }

                    if (listLength(clients) == 0) {
                        dictDelete(rl->db->blocking_keys, rl->key);
                        break;
                    }

                }
            }

            decrRefCount(rl->key);
            listDelNode(l, node);
            zfree(node);
            zfree(rl);
            node = NULL;
        }

    }


}

void disconnectAllBlockedClients(void) {


}
