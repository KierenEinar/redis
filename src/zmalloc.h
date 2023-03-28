//
// Created by kieren jiang on 2023/3/27.
//

#ifndef REDIS_ZMALLOC_H
#define REDIS_ZMALLOC_H

#include <sys/types.h>
void* zmalloc(size_t size);
void* zcalloc(size_t count, size_t size);
void* zrealloc(void* ptr, size_t size);
size_t zmalloc_used_memory();

void zfree(void *ptr);

#endif //REDIS_ZMALLOC_H
