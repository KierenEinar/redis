//
// Created by kieren jiang on 2023/8/21.
//

#include "server.h"


void subscribeCommand(client *c) {
    int j;
    for (j = 1; j < c->argc; j++) {
        pubsubSubscribeChannel(c, c->argv[j]);
    }
}


void psubscribeCommand(client *c) {
    int j;
    for (j = 1; j < c->argc; j++) {
        pubsubSubscribePattern(c, c->argv[j]);
    }
}


void publishCommand(client *c) {
    pubsubPublishMessage(c->argv[1], c->argv[2]);
}


int pubsubSubscribeChannel(client *c, robj *channel) {

    int retval = 0;
    list *l;
    if (dictFind(c->pubsub_channels, channel->ptr) == NULL) {

        channel = getDecodedObject(channel);
        dictAdd(c->pubsub_channels, channel, NULL);
        l = dictFetchValue(server.pubsub_channels, channel->ptr);
        if (l == NULL) {
            l = listCreate();
            dictAdd(server.pubsub_channels, channel->ptr, l);
            incrRefCount(channel);
        }

        listAddNodeTail(l, c);
        decrRefCount(channel);
        retval = 1;
    }

    return retval;
}

// subscribe pattern except glob like style input.
int pubsubSubscribePattern(client *c, robj *pattern) {

    int retval = 0;
    pubsubPattern *pat;
    if (listSearchKey(c->pubsub_patterns, pattern) == NULL) {

        incrRefCount(pattern);
        listAddNodeTail(c->pubsub_patterns, pattern);

        pat = zmalloc(sizeof(*pat));
        pat->client = c;
        pat->pattern = pattern;
        listAddNodeTail(server.pubsub_patterns, pat);

        incrRefCount(pattern);
    }
    return 0;

}

// publish message to all subscribe clients.
int pubsubPublishMessage(robj *channel, robj *message) {

    list *l;
    listIter liter;
    listNode *node;
    int receivers = 0;

    if ((l = dictFetchValue(server.pubsub_channels, channel)) != NULL) {
        listRewind(l, &liter);
        while ((node = listNext(&liter)) != NULL) {
            client *c = node->value;
            addReply(c, shared.mbulkhdr[3]);
            addReply(c, shared.subscribe);
            addReplyBulk(c, channel);
            addReplyBulk(c, message);
            receivers++;
        }
    }

    if (listLength(server.pubsub_patterns)) {
        l = server.pubsub_patterns;
        listRewind(l, &liter);
        while ((node = listNext(&liter)) != NULL) {
            pubsubPattern *pat = node->value;
            sds pattern = pat->pattern->ptr;
            client *c = pat->client;
            if (matchstringlen(pattern,
                               (int)sdslen(pattern),
                               channel->ptr,
                               (int)sdslen(channel->ptr),
                               0)) {

                addReply(c, shared.mbulkhdr[3]);
                addReply(c, shared.psubscribe);
                addReplyBulk(c, pat->pattern);
                addReplyBulk(c, message);

            }
            receivers++;
        }

    }

    return receivers;

}

unsigned long clientSubscriptionCount(client *c) {
    return dictSize(c->pubsub_channels) +
            listLength(c->pubsub_patterns);
}