//
// Created by kieren jiang on 2023/7/6.
//

#include "server.h"

void ttlGenericCommand(client *c, int output_ms) {
    long long expire, ttl = -1;

    // key not exists, just return -2
    if (lookupKeyRead(c->db, c->argv[1])==NULL) {
        addReplyLongLong(c, -2);
        return;
    }

    expire = getExpire(c->db, c->argv[1]);
    fprintf(stdout, "ttl=%lld\r\n", expire);
    if (expire != -1) {
        ttl = expire - mstime();
        if (ttl < 0) ttl = 0;
    }

    if (ttl == -1) {
        addReplyLongLong(c, -1);
    } else {
        addReplyLongLong(c, output_ms ? ttl : (ttl +500) / 1000);
    }
}


void ttlCommand(client *c) {
    ttlGenericCommand(c, 0);
}

void pttlCommand(client *c) {
    ttlGenericCommand(c, 1);
}
