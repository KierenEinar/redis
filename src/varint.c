//
// Created by kieren jiang on 2023/3/5.
//

#include "varint.h"
#include <memory.h>
#include <stdlib.h>
#include <stdio.h>
char *putUVarInt64(unsigned long long value) {
    char buf[10];
    int i=0, v = 0;
    while (value > 0x7f) {
        v = (value & 0x7f) | 0x80;
        buf[i++] = v;
        value = value >> 7;
    }
    buf[i++] = value & 0x7f;
    char *binary = (void *)malloc(sizeof(char) * i);
    memcpy(binary, buf, i);
    return binary;
}

unsigned long long uVarInt64(const char* c) {
    unsigned long long value = 0;
    int i = 0;
    while (1) {
        unsigned long long v = c[i] & 0x7f;
        value = value | (v << 7*i);
        if (!(c[i++] & 0x80)) {
            break;
        }
    }
    return value;
}

char *putVarInt64(long long value) {
    long long ux = (unsigned long long)(value) << 1;
    if (value < 0) {
        ux = ~ux;
    }
    return putUVarInt64(ux);
}

long long varInt64(const char *c) {
    unsigned long long ux = uVarInt64(c);
    long long x = ux >> 1;
    if (ux&1) { // negative
        x = ~x;
    }
    return x;
}