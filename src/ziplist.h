//
// Created by kieren jiang on 2023/7/11.
//

#ifndef REDIS_ZIPLIST_H
#define REDIS_ZIPLIST_H

#include <sys/types.h>
#include <stdint.h>


void memrev32(void *p);
uint32_t int32rev(uint32_t v);

void memrev16(void *p);
uint16_t int16rev(uint16_t v);

#if (BYTE_ORDER == LITTLE_ENDIAN)
#define int16revifbe(v) (v)
#define int32revifbe(v) (v)
#else
#define int16revifbe(v) int16rev(v)
#define int32revifbe(v) int32rev(v)
#endif

#define ZIP_LIST_ERR 0
#define ZIP_LIST_OK 1


#define ZIP_LIST_END 255

#define ZIP_INT_08B  0xe0 // 11100000
#define ZIP_INT_16B  0xd0 // 11010000
#define ZIP_INT_32B  0xc8 // 11001000
#define ZIP_INT_64B  0xc4 // 11000100

#define ZIP_STR_MASK 0xc0 // 11000000

#define ZIP_STR_06B  0<<6
#define ZIP_STR_14B  1<<6
#define ZIP_STR_32B  1<<7

#define ZIP_IS_STR(encoding) ((encoding) & ZIP_STR_MASK) < ZIP_STR_MASK

#define ZIP_PREVLEN_08B 254

// insert s at 'p'
unsigned char *__ziplistInsert(unsigned char *zl, unsigned char *p, unsigned char *s, unsigned int slen);

#endif //REDIS_ZIPLIST_H
