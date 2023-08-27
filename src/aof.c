//
// Created by kieren jiang on 2023/8/27.
//
#include "server.h"

sds catAppendOnlyFileExpireCommand(sds buf, struct redisCommand *cmd, robj *key, robj *expire) {

    long long when;
    robj *tmpargv[3];
    int j;

    expire = getDecodedObject(expire);
    string2ll(expire->ptr, sdslen(expire->ptr), &when);
    decrRefCount(expire);

    if (cmd->proc == expireCommand) {
        when *= 1000;
    }

    if (cmd->proc == expireCommand || cmd->proc == pexpireCommand) {
        when += mstime();
    }

    tmpargv[0] = createStringObject("PEXPIREAT", 9);
    tmpargv[1] = key;
    tmpargv[2] = createStringObjectFromLongLong(when);
    buf  = catAppendOnlyFileGenericCommand(buf, 3, tmpargv);

    decrRefCount(tmpargv[0]);
    decrRefCount(tmpargv[2]);

    return buf;

}

sds catAppendOnlyFileGenericCommand(sds buf, int argc, robj **argv) {

    int j;

    buf = sdscatfmt(buf, "*%i\r\n", argc);

    for (j = 0; j < argc; j++) {
        robj *obj = argv[j];
        obj = getDecodedObject(obj);
        buf = sdscatfmt(buf, "$%i\r\n", sdslen(obj->ptr));
        buf = sdscatsds(buf, obj->ptr);
        buf = sdscatfmt(buf, shared.crlf->ptr);
        decrRefCount(obj);
    }

    return buf;
}

void feedAppendOnlyFile(struct redisCommand *cmd, long dbid, int argc, robj **argv) {

    sds buf = sdsempty();

    if (dbid != server.aof_seldb) {
        char db_buf[64];
        snprintf(db_buf, sizeof(db_buf), "%ld", dbid);
        buf = sdscatfmt(buf, "*2\r\n$6\r\nSELECT\r\n$%U\r\n%i\r\n", strlen(db_buf), dbid);
        server.aof_seldb = dbid;
    }

    if (cmd->proc == expireCommand || cmd->proc == pexpireCommand) {

        buf = catAppendOnlyFileExpireCommand(buf, cmd, argv[1], argv[2]);

    } else if (cmd->proc == setCommand && argc > 3) {

        robj *exargv, *pxargv;

        exargv = NULL;
        pxargv = NULL;

        robj *tempargvs[3];

        for (int j = 3; j < argc; j++) {
            robj *tmpargv = getDecodedObject(argv[j]);
            if (strcasecmp(tmpargv->ptr, "ex") == 0) exargv = argv[j+1];
            if (strcasecmp(tmpargv->ptr, "px") == 0) pxargv = argv[j+1];
            decrRefCount(tmpargv);
        }

        tempargvs[0] = argv[0];
        tempargvs[1] = argv[1];
        tempargvs[2] = argv[2];

        catAppendOnlyFileGenericCommand(buf, 3, tempargvs);

        if (exargv)
            catAppendOnlyFileExpireCommand(buf, server.expireCommand, argv[1], exargv);
        if (pxargv)
            catAppendOnlyFileExpireCommand(buf, server.pexpireCommand, argv[1], pxargv);
    }  else {
        catAppendOnlyFileGenericCommand(buf, argc, argv);
    }

    server.aof_buf = sdscatsds(server.aof_buf, buf);

    sdsfree(buf);

}

