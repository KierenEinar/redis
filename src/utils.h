//
// Created by kieren jiang on 2023/3/20.
//

#ifndef REDIS_UTILS_H
#define REDIS_UTILS_H

#define LLMAXSIZE 21
#include <sys/types.h>
#include "zmalloc.h"
#define RESP_PROTO_MAX_INLINE_SEG (1024 * 64)
#define RESP_PROTO_MAX_BULK_SEG (512 * 1024 * 1024)
#define RESP_PROCESS_ERR (-1)
#define RESP_PROCESS_OK (0)

int string2ll(const char *s, size_t slen, long long *value);
int string2l(const char *s, size_t slen, long *value);
// slen including '\0'
size_t ll2string(char *s, long long value);
size_t ull2string(char *s, unsigned long long value);
// split string to tokens
char** stringsplitargs(const char *line, int *argc);

// reverse long value
// http://graphics.stanford.edu/~seander/bithacks.html#ReverseParallel
unsigned long u_rev(unsigned long value);

unsigned long long uu_rev(unsigned long long value);

#endif //REDIS_UTILS_H
