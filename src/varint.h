//
// Created by kieren jiang on 2023/3/5.
//

#ifndef REDIS_UVARINT_H
#define REDIS_UVARINT_H

#include <sys/types.h>

char *putUVarInt64(unsigned long long value);
unsigned long long uVarInt64(const char* c);

#endif //REDIS_UVARINT_H
