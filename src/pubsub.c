//
// Created by kieren jiang on 2023/8/21.
//

#include "server.h"


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
    return 0;
}

// publish message to all subscribe clients.
int pubsubPublishMessage(robj *channel, robj *message) {
    return 0;
}