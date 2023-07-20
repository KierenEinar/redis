//
// Created by kieren jiang on 2023/7/11.
//

#ifndef REDIS_ZIPLIST_H
#define REDIS_ZIPLIST_H

#include <sys/types.h>
#include <stdint.h>

#define ZIPLIST_INSERT_HEAD 0
#define ZIPLIST_INSERT_TAIL 1

void memrev16(void *p);
void memrev32(void *p);
void memrev64(void *p);
uint16_t int16rev(uint16_t v);
uint32_t int32rev(uint32_t v);
uint64_t int64rev(uint64_t v);


#define ZIPLIST_HEADER (sizeof(uint32_t)*2+sizeof(uint16_t))
#if (BYTE_ORDER == LITTLE_ENDIAN)
#define int16revifbe(v) (v)
#define int32revifbe(v) (v)
#define int64revifbe(v) (v)
#define memrev16ifbe(v)
#define memrev32ifbe(v)
#define memrev64ifbe(v)

#else
#define int16revifbe(v) int16rev(v)
#define int32revifbe(v) int32rev(v)
#define int64revifbe(v) int64rev(v)
#define memrev16ifbe(v) memrev16(v)
#define memrev32ifbe(v) memrev32(v)
#define memrev64ifbe(v) memrev64(v)
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

// create the ziplist.
unsigned char *ziplistNew(void);
// return the next entry from p.
unsigned char *ziplistNext(unsigned char *zl, unsigned char *p);
// return the prev entry from p.
unsigned char *ziplistPrev(unsigned char *zl, unsigned char *p);
// push head or tail to the ziplist.
unsigned char *ziplistPush(unsigned char *zl, unsigned char *s, size_t slen, int where);
// get the index of entry ptr.
unsigned char *ziplistIndex(unsigned char *zl, int index);
// get the entry data. one of sstr
unsigned int ziplistGet(unsigned char *p, unsigned char **sstr, unsigned int *slen, long long *value);
// find the ptr that data eq str.
unsigned char *ziplistFind(unsigned char *p, unsigned char *str, unsigned int slen, unsigned int skipcnt);
// delete range, start by index
unsigned char *ziplistDeleteRange(unsigned char *zl, int index, unsigned int num);
// pretty print
void ziplistRepr(unsigned char *zl);

// insert s at 'p'.
unsigned char *__ziplistInsert(unsigned char *zl, unsigned char *p, unsigned char *s, unsigned int slen);

// delete at p
unsigned char *__ziplistDelete(unsigned char *zl, unsigned char *p, unsigned int num);

#endif //REDIS_ZIPLIST_H
