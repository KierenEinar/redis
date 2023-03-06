//
// Created by kieren jiang on 2023/2/20.
//

#include "redis.h"
#include "varint.h"
#include <stdio.h>
#include <stdlib.h>
#include <limits.h>
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

    free(c2);
    free(c);

    int c3 = 13;
    c3 = c3 & (~(1<<2));
    printf("%d\n", c3);

    sds s4 = sdsll2str(LONG_LONG_MAX);
    size_t s4len;
    char *s4buf = sdsstr(s4, &s4len);
    printf("s4_buf=%s, s4_len=%ld\n", s4buf, s4len);
    sdsfree(s4);

    sds s5 = sdsll2str(LONG_LONG_MIN);
    size_t s5len;
    char *s5buf = sdsstr(s5, &s5len);
    printf("s5_buf=%s, s5_len=%ld\n", s5buf, s5len);
    sdsfree(s5);

    sds s6 = sdsull2str(LONG_LONG_MAX);
    size_t s6len;
    char *s6buf = sdsstr(s6, &s6len);
    printf("s6_buf=%s, s6_len=%ld\n", s6buf, s6len);
    sdsfree(s6);

    sds s7 = sdsull2str(ULLONG_MAX);
    size_t s7len;
    char *s7buf = sdsstr(s7, &s7len);
    printf("s7_buf=%s, s7_len=%ld\n", s7buf, s7len);
    sdsfree(s7);


}