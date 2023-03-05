//
// Created by kieren jiang on 2023/2/20.
//

#include "redis.h"
#include "dict.h"
#include "varint.h"
#include <stdio.h>
#include <stdlib.h>
int main(int argc, char **argv) {
    unsigned long long value = 18446744073709551615;
    char *c = putUVarInt64(value);
    printf("c = %s\n", c);
    unsigned long long nv= uVarInt64(c);
    printf("nv = %llu\n", nv);
    free(c);
}