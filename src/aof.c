//
// Created by kieren jiang on 2023/5/6.
//

#include "server.h"

void catAppendOnlyExpireAtCommand(struct redisCommand *cmd, robj *key, robj *seconds) {

    long long when;

    seconds = getDecodedObject(seconds);


}


void feedAppendOnlyFile(struct redisCommand *cmd, int seldb, robj **argv, int argc) {

    if (cmd->proc == expireCommand || cmd->proc == expireAtCommand ||
        cmd->proc == pexpireCommand || cmd->proc == pexpireAtCommand ) {

        catAppendOnlyExpireAtCommand(cmd, argv[1], argv[2]);
    } else if (cmd->proc == setexCommand) {




    } else if (cmd->proc == setCommand && argc > 3) {

    } else {

    }


}

