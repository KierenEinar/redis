//
// Created by kieren jiang on 2023/2/20.
//

#include "redis.h"
#include "dict.h"
#include "varint.h"
#include <stdio.h>
#include <stdlib.h>
int main(int argc, char **argv) {
    unsigned long long value = 18446744073709551615l;
    char *c = putUVarInt64(value);
    printf("c = %s\n", c);
    unsigned long long nv= uVarInt64(c);
    printf("nv = %llu\n", nv);

    long long value2 = -9223372036854775808l;
    char *c2 = putVarInt64(value2);
    printf("c2 = %s\n", c2);
    long long nv2= varInt64(c2);
    printf("nv = %lld\n", nv2);

    free(c2);
    free(c);
}