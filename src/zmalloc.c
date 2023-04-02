//
// Created by kieren jiang on 2023/3/27.
//

#include "zmalloc.h"
#include "stdlib.h"

void* zmalloc(size_t size) {
    return malloc(size);
}

void* zcalloc(size_t count, size_t size) {
    return calloc(count, size);
}

void* zrealloc(void *ptr, size_t size) {
    return realloc(ptr, size);
}

size_t zmalloc_used_memory () {
    return 0; // todo add used memory
}

void zfree(void *ptr) {
    if (!ptr) return;
    free(ptr);
}