//
// Created by kieren jiang on 2023/8/23.
//

#include "server.h"

void initClientMultiState(client *c) {
    c->mstate.count = 0;
    c->mstate.multi_cmds = NULL;
}

void freeClientMultiState(client *c) {

    for (int j=0; j<c->mstate.count; j++) {
        multiCmd *mc;
        mc = c->mstate.multi_cmds[j];
        for (int i=0; i<mc->argc; i++) {
            decrRefCount(mc->argv[i]);
        }
        zfree(mc->argv);
    }

    zfree(c->mstate.multi_cmds);

}

void flagTransactionAsDirty(client *c) {
    if (c->flag & CLIENT_MULTI) {
        c->flag |= CLIENT_DIRTY_EXEC;
    }
}

// start the transaction.
void multiCommand(client *c) {
    if (c->flag & CLIENT_MULTI) {
        addReplyError(c, "multi not support nested");
        return;
    }
    c->flag |= CLIENT_MULTI;
    addReply(c, shared.ok);
}

void queueMultiCommand(client *c) {

    c->mstate.multi_cmds = zrealloc(c->mstate.multi_cmds, sizeof(multiCmd*) * (c->mstate.count + 1));
    c->mstate.count++;

    multiCmd *cmd = c->mstate.multi_cmds[c->mstate.count - 1];
    cmd->argc = c->argc;
    cmd->argv = zmalloc(sizeof(robj*) * c->argc);
    memcpy(cmd->argv, c->argv, sizeof(robj*) * c->argc);
    for (int j=0; j<c->argc; j++) {
        incrRefCount(cmd->argv[j]);
    }
    addReply(c, shared.queued);
}

void watchForKey(client *c, robj *key) {

    list *clients;
    listIter iter;
    listNode *node;


    listRewind(c->watch_keys, &iter);

    while ((node = listNext(&iter)) != NULL) {
        watchKey *cwk = node->value;
        if (cwk->db == c->db && listValueEqual(cwk->key, key)) {
            return;
        }
    }


    watchKey *wk = zmalloc(sizeof(*wk));
    wk->db = c->db;
    wk->key = key;

    listAddNodeTail(c->watch_keys, wk);
    incrRefCount(key);

    clients = dictFetchValue(c->db->watch_keys, key);
    if (!clients) {
        clients = listCreate();
        dictAdd(c->db->watch_keys, key, clients);
        incrRefCount(key);
    }

    listAddNodeTail(clients, c);

}

void unWatchAllKeys(client *c) {

    listIter iter;
    listNode *node;

    if (listLength(c->watch_keys) == 0)
        return;

    listRewind(c->watch_keys, &iter);
    while ((node = listNext(&iter)) != NULL) {

        watchKey *wk = node->value;

        list *clients = dictFetchValue(wk->db->watch_keys, wk->key);

        listDelNode(clients, listSearchKey(clients, c));

        if (listLength(clients) == 0) {
            listRelease(clients);
            dictDelete(wk->db->watch_keys, wk->key);
            decrRefCount(wk->key);
        }

        listDelNode(c->watch_keys, node);
        decrRefCount(wk->key);
        zfree(wk);
    }

}

void discardTransaction(client *c) {

    freeClientMultiState(c);
    initClientMultiState(c);
    unWatchAllKeys(c);
    c->flag &= ~(CLIENT_MULTI | CLIENT_DIRTY_EXEC | CLIENT_CAS_EXEC);
}

// watch the keys.
void watchCommand(client *c) {

    if (c->flag & CLIENT_MULTI) {
        addReplyError(c, "watch must not in multi context.");
        return;
    }

    for (int j=1; j<c->argc; j++) {
        watchForKey(c, c->argv[j]);
    }

    addReply(c, shared.ok);
}

// cancel watch the key
void unWatchCommand(client *c) {

    unWatchAllKeys(c);
    c->flag &= ~CLIENT_CAS_EXEC;
    addReply(c, shared.ok);
}

// discard the transaction.
void discardCommand(client *c) {

    if (!(c->flag & CLIENT_MULTI)) {
        addReplyError(c,"DISCARD without MULTI");
        return;
    }

    discardTransaction(c);
    addReply(c, shared.ok);
}

// exec the transaction.
void execCommand(client *c) {




}

void touchWatchedKey(redisDb *db, robj *key) {

    list *clients;
    listIter iter;
    listNode *ln;

    clients = dictFetchValue(db->watch_keys, key);
    if (!clients) {
        return;
    }

    listRewind(clients, &iter);

    while ((ln = listNext(&iter)) != NULL) {
        client *c = ln->value;
        c->flag |= CLIENT_CAS_EXEC;
    }

}