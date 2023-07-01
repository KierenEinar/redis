//
// Created by kieren jiang on 2023/6/30.
//

#include "server.h"

int getGenericCommand(client *c) {
//    robj *value = lookupKeyReadOrReply(c->argv[1], shared.nullbulk);
//    if (value == NULL) return C_ERR;
    return C_ERR;
}


void getCommand(client *c) {
    getGenericCommand(c);
}
