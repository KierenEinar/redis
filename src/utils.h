//
// Created by kieren jiang on 2023/3/20.
//

#ifndef REDIS_UTILS_H
#define REDIS_UTILS_H

#define LLMAXSIZE 21
#include <sys/types.h>

int string2ll(const char *s, size_t slen, long long *value);
int string2l(const char *s, size_t slen, long *value);

// slen including '\0'
size_t ll2string(char *s, size_t slen, long long value);


#endif //REDIS_UTILS_H
