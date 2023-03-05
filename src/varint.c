//
// Created by kieren jiang on 2023/3/5.
//

#include "varint.h"
#include <memory.h>
#include <stdlib.h>
#include <stdio.h>
char *putUVarInt64(unsigned long long value) {
    char buf[10];
    int i, v = 0;
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