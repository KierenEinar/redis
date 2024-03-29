//
// Created by kieren jiang on 2023/2/20.
//

#include "server.h"
#include "varint.h"
#include <limits.h>

uint64_t dictSdsHash(const void *key) {
    return 2ul;
}

int dictSdsComparer(const void *key1, const void *key2) {
    return 1;
}

void dictObjectDestructor(void *data) {

}

void dictSdsDestructor(void *data) {

}

// global vars
dictType dbDictType = {
        dictSdsHash,
        NULL,
        NULL,
        dictSdsComparer,
        dictSdsDestructor,
        dictObjectDestructor,
};

struct redisServer server;

mstime_t mstime() {
    struct timeval t;
    gettimeofday(&t, NULL);
    mstime_t milliseconds= 0;
    milliseconds = t.tv_sec * 1000 + t.tv_usec / 1000;
    return milliseconds;
}


void exitFromChild(int code) {
    _exit(code);
}


int main(int argc, char **argv) {
    unsigned long long value = 18446744073709551615UL;
    char *c = putUVarInt64(value);
    printf("c = %s\n", c);
    unsigned long long nv= uVarInt64(c);
    printf("nv = %llu\n", nv);

    long long value2 = 9223372036854775807l;
    char *c2 = putVarInt64(value2);
    printf("c2 = %s\n", c2);
    long long nv2= varInt64(c2);
    printf("nv = %lld\n", nv2);

    int c3 = 13;
    c3 = c3 & (~(1<<2));
    printf("%d\n", c3);

    sds s4 = sdsll2str(LONG_LONG_MAX);
    size_t s4len;
    char *s4buf = sdsstr(s4, &s4len);
    printf("s4_buf=%s, s4_len=%ld\n", s4buf, s4len);


    sds s5 = sdsll2str(LONG_LONG_MIN);
    size_t s5len;
    char *s5buf = sdsstr(s5, &s5len);
    printf("s5_buf=%s, s5_len=%ld\n", s5buf, s5len);


    sds s6 = sdsull2str(LONG_LONG_MAX);
    size_t s6len;
    char *s6buf = sdsstr(s6, &s6len);
    printf("s6_buf=%s, s6_len=%ld\n", s6buf, s6len);


    sds s7 = sdsull2str(ULLONG_MAX);
    size_t s7len;
    char *s7buf = sdsstr(s7, &s7len);
    printf("s7_buf=%s, s7_len=%ld\n", s7buf, s7len);

    sds s8 = sdscatsprintf(s7, "%lld-%s", 123, "hello world");
    size_t s8len;
    char *s8buf = sdsstr(s8, &s8len);
    printf("s8_buf=%s, s8_len=%ld\n", s8buf, s8len);


    sds s9 = sdscatfmt(sdsempty(), "hello, %i", 1);


    free(c2);
    free(c);
    sdsfree(s4);
    sdsfree(s5);
    sdsfree(s6);
    sdsfree(s7);
    sdsfree(s8);


}
