//
// Created by kieren jiang on 2023/7/11.
//

/*
 * ziplist overall layout
 * <zlbytes><zltail><zllen><entry><entry>...<zlend>
 * <uint32_t zlbytes> is an unsigned integer that holds the number
 * of bytes the ziplist occupies. including the four bytes its field.
 *
 * <uint32_t zltail> is the offset last entry in this list.
 * <uint16_t zllen> is the number of entries. when there are more than 2^16-2 entries,
 * the value is set to 2^16-1, so we need to traverse the entire list
 * to know how many items it holds.
 * <uint8_t zlend> is a special entry representing the end of list.
 * Is encoded as a single byte equal to 255.
 * no other entry start with a byte set to the value of 255.
 *
 *
 * entry overall layout
 * <prevlen><encoding><entry-data>
 *
 */

#include "ziplist.h"
#include "utils.h"

#include <stdlib.h>
#include <memory.h>

void memrev16(void *p) {
    unsigned char *x = p, t;
    t = x[0];
    x[0] = x[1];
    x[1] = t;
}

void memrev32(void *p) {
    unsigned char *x=p, t;
    t = x[3];
    x[3] = x[0];
    x[0] = t;
    t = x[2];
    x[2] = x[1];
    x[1] = t;
}

void memrev64(void *p) {
    unsigned char *x=p, t;
    t = x[7];
    x[7] = x[0];
    x[0] = t;
    t = x[6];
    x[6] = x[1];
    x[1] = t;
    t = x[5];
    x[5] = x[2];
    x[2] = t;
    t = x[4];
    x[4] = x[3];
    x[3] = t;
}

uint16_t int16rev(uint16_t v) {
    memrev16(&v);
    return v;
}

uint32_t int32rev(uint32_t v) {
    memrev32(&v);
    return v;
}

uint64_t int64rev(uint64_t v) {
    memrev64(&v);
    return v;
}

uint32_t ziplistBytesLen(unsigned char *zl) {
    uint32_t *ptr = (uint32_t*)(zl);
    return int32revifbe(*ptr);
}

uint32_t ziplistTailOffset(unsigned char *zl) {
    uint32_t offset;
    memcpy(&offset, zl+4, 4);
    return int32revifbe(offset);
}

int zipTryEncoding(unsigned char *s, unsigned int slen, uint8_t *encoding, long long *value) {

    long long v;
    uint8_t enc;
    if (slen > 20) return ZIP_LIST_ERR;
    if (string2ll((const char*)s, slen, &v) == 0) return ZIP_LIST_ERR;
    if (value) *value = v;

    // 11 100000 1byte int8
    // 11 010000 2byte int16
    // 11 001000 4byte int32
    // 11 000100 8byte int64

    if (INT8_MIN <= v && v <= INT8_MAX) {
        enc = ZIP_INT_08B;
    } else if (INT16_MIN <= v && v <= INT16_MAX) {
        enc = ZIP_INT_16B;
    } else if (INT32_MIN <= v && v <= INT32_MAX) {
        enc = ZIP_INT_32B;
    } else {
        enc = ZIP_INT_64B;
    }

    if (encoding) *encoding = enc;

    return ZIP_LIST_OK;
}

int zipIntSize(uint8_t encoding) {
    if (encoding == ZIP_INT_08B) {
        return 1;
    } else if (encoding == ZIP_INT_16B) {
        return 2;
    } else if (encoding == ZIP_INT_32B) {
        return 4;
    } else if (encoding == ZIP_INT_64B) {
        return 8;
    } else {
        // todo sererv panic
        exit(-1);
    }
}


int zipStoreEntryEncoding(unsigned char *p, uint8_t encoding, unsigned int slen) {

    uint8_t reqlen = 1;

    // encoding store by big endian

    if (ZIP_IS_STR(encoding)) {

        if (slen <= 0x3f) {
            if (!p) return reqlen;
            p[0] = slen&0xff;

        } else if (slen <= 0x3fff) {
            reqlen+=1;
            if (!p) return reqlen;
            p[0] = slen >> 8 & 0x3f | 0x40;
            p[1] = slen & 0xff;
        } else {
            reqlen+=4;
            if (!p) return reqlen;
            p[0] = 0x80;
            p[1] = slen >> 24 & 0xff;
            p[2] = slen >> 16 & 0xff;
            p[3] = slen >> 8 & 0xff;
            p[4] = slen & 0xff;
        }
    } else {

        if (!p) return reqlen;
        p[0] = encoding;
    }

    // int encoding reqlen is 1.
    return reqlen;

}

int zipStoreEntryPrevLen(unsigned char *p, unsigned int prevlen) {

    if (prevlen < ZIP_PREVLEN_08B) {
        if (p) p[0] = prevlen & 0xff;
        return 1;
    } else {
        if (p) {
            p[0] = ZIP_PREVLEN_08B;
            prevlen = int32revifbe(prevlen);
            memcpy(p+1, &prevlen, 4);
        }
        return 5;
    }
}

int zipStoreEntryPrevLenLarge(unsigned char *p, unsigned int prevlen) {

    p[0] = ZIP_PREVLEN_08B;
    prevlen = int32revifbe(prevlen);
    memcpy(p+1, &prevlen, 4);
    return 5;
}


int zipDecodePrevlensSize(unsigned char *p) {
    if (p[0] < ZIP_PREVLEN_08B) {
        return 1;
    } else {
        return 5;
    }
}

uint32_t zipDecodeEntryPrevLen(unsigned char *p, int *prevlensize) {
    int _prevlensize = zipDecodePrevlensSize(p), prevlen;
    if (_prevlensize == 1) {
        prevlen = p[0];
    } else {
        memcpy(&prevlen, p+1, 4);
        prevlen = int32revifbe(prevlen);
    }
    if (prevlensize) *prevlensize = _prevlensize;
    return prevlen;

}

unsigned char *zipEntryTail(unsigned char *zl) {

    uint32_t tailOffset;
    memcpy(&tailOffset, zl+4, sizeof(uint32_t));
    tailOffset = int32revifbe(tailOffset);
    return zl+tailOffset;
}

unsigned char zipTryDecodeEncoding(unsigned char *p) {
    if ((p[0] & ZIP_STR_MASK) < ZIP_STR_MASK) {
        return p[0] & ZIP_STR_MASK;
    }
    return p[0];
}

uint32_t zipRawEntryLength(unsigned char *p) {

   // encoding str: prevlensize + encoding + rawlensize(exists when rawlen > 63) + rawlen

   // encoding int: prevlensize + encoding + intxxbit_byte

   uint32_t rawlensize, rawlen;
   unsigned char encoding;
   int prevlensize;
   zipDecodeEntryPrevLen(p, &prevlensize);
   encoding = zipTryDecodeEncoding(p+prevlensize);

   if (ZIP_IS_STR(encoding)) {
       if (encoding == ZIP_STR_06B) {
           rawlensize = 1;
           p = p + prevlensize + rawlensize;
           rawlen = ~ZIP_STR_MASK & p[0];
       } else if (encoding == ZIP_STR_14B) {
           rawlensize = 2;
           p = p + prevlensize + rawlensize;
           rawlen = ~ZIP_STR_MASK & p[0] << 8 | p[1];
       } else {
           rawlensize = 5;
           p = p + prevlensize + rawlensize;
           rawlen = p[1] << 24 | p[2] << 16 | p[3] << 8 | p[4];
       }
   } else {
       rawlensize = 1;
       if (encoding == ZIP_INT_08B) {
           rawlen = 1;
       } else if (encoding == ZIP_INT_16B) {
           rawlen = 2;
       } else if (encoding == ZIP_INT_32B) {
           rawlen = 4;
       } else {
           rawlen = 8;
       }
   }

   return prevlensize + rawlensize + rawlen;
}

int zipEntryPrevlenBytesDiff(unsigned char *p, unsigned int prevlen) {
    int prevlensize;
    zipDecodeEntryPrevLen(p, &prevlensize);
    return zipStoreEntryPrevLen(NULL, prevlen) -  prevlensize;
}

void ziplistStoreByteslen(unsigned char *zl, uint32_t byteslen) {
    byteslen = int32revifbe(byteslen);
    memcpy(zl, &byteslen, 4);
}


void ziplistStoreTailOffset(unsigned char *zl, uint32_t tail) {
    tail = int32revifbe(tail);
    memcpy(zl+4, &tail, 4);
}


void ziplistStoreLength(unsigned char *zl, uint16_t length) {
    length = int32revifbe(length);
    memcpy(zl+8, &length, 2);
}

unsigned char *ziplistResize(unsigned char *zl, uint32_t newsize) {
    zrealloc(zl, newsize);
    zl[newsize] = ZIP_LIST_END;
    ziplistStoreByteslen(zl, newsize);
    return zl;
}

void ziplistSaveInteger(unsigned char *p, unsigned char encoding, long long value) {

    int8_t  i8;
    int16_t i16;
    int32_t i32;

    if (encoding == ZIP_INT_08B) {
        i8 = (int8_t)value;
        memcpy(p, &i8, 1);
    } else if (encoding == ZIP_INT_16B) {
        i16 = (int16_t)value;
        memcpy(p, &i16, 2);
        memrev16ifbe(&i16);
    } else if (encoding == ZIP_INT_32B) {
        i32 = (int32_t)value;
        memcpy(p, &i32, 4);
        memrev32ifbe(&i32);
    } else {
        memcpy(p, &value, 8);
        memrev64ifbe(&value);
    }
}

void ziplistLoadInteger(unsigned char *p, long long *value) {

    unsigned char encoding;
    int prevlensize;
    int8_t *i8;
    int16_t *i16;
    int32_t *i32;
    int64_t *i64;

    prevlensize = zipDecodePrevlensSize(p);
    p+=prevlensize;
    encoding = p[0];
    p+=1;
    if (encoding == ZIP_INT_08B) {
        i8 = (int8_t*)p;
        *value = (long long)(*i8);
    } else if (encoding == ZIP_INT_16B) {
        i16 = (int16_t*)p;
        memrev16ifbe(&i16);
        *value = (long long)(*i16);
    } else if (encoding == ZIP_INT_32B) {
        i32 = (int32_t*)p;
        memrev32ifbe(&i16);
        *value = (long long)(*i32);
    } else {
        i64 = (int64_t*)p;
        memrev64ifbe(&i64);
        *value = (long long)(*i64);
    }

}

void ziplistIncrLength(unsigned char *zl) {

    uint16_t length;
    memcpy(&length, zl+8, 2);
    length = int16rev(length);
    if (length < 0xffff) {
        length = int16rev(length+1);
        memcpy(&length, zl+8, 2);
    }
}

unsigned char *ziplistHeader(unsigned char *zl) {
    return zl + HDRSIZE;
}

unsigned char *ziplistTail(unsigned char *zl) {
    return zl + ziplistTailOffset(zl);
}


unsigned char *__ziplistCascadeUpdate(unsigned char *zl, unsigned char *p) {

    uint32_t rawlen, prevlen, rawlensize, curlen, offset;

    int nextdiff, prevlensize;

    curlen = ziplistBytesLen(zl);

    while (p[0] != ZIP_LIST_END) {

        rawlen = zipRawEntryLength(p);
        if (p[rawlen] == ZIP_LIST_END) {
            break;
        }

        prevlen = zipDecodeEntryPrevLen(p+rawlen, &prevlensize);
        if (prevlen == rawlen) {
            break;
        }

        rawlensize = zipStoreEntryPrevLen(NULL, rawlen);

        if (prevlensize < rawlensize) {
            nextdiff = rawlensize - prevlensize;
            offset = p - zl;
            zl = ziplistResize(zl, curlen+nextdiff);
            p = zl + offset;

            // update tail offset
            if (zl + ziplistTailOffset(zl) == p+rawlen) {

            } else {
                ziplistStoreTailOffset(zl, ziplistTailOffset(zl) + nextdiff);
            }

            memmove(p+rawlen, p+rawlen-nextdiff, curlen-offset+nextdiff-rawlen-1);
            zipStoreEntryPrevLen(p+rawlen, rawlen);
            p = p+rawlen;
            curlen+=nextdiff;

        } else {

            if (prevlensize > rawlensize) {
                zipStoreEntryPrevLenLarge(p+rawlen, rawlensize);
            } else {
                zipStoreEntryPrevLen(p+rawlen, rawlensize);
            }
            break;
        }

    }

    return zl;


}


unsigned char *__ziplistInsert(unsigned char *zl, unsigned char *p, unsigned char *s, unsigned int slen) {

    uint32_t reqlen, offset, tailoffset, prevlen = 0, curlen = ziplistBytesLen(zl);
    long long value;
    uint8_t encoding = 0;
    int nextdiff, prevlensize, forcelarge = 0;

    // calculate prevlensize
    if (p[0] != ZIP_LIST_END) {
        prevlen = zipDecodeEntryPrevLen(p, &prevlensize);
    } else {
        unsigned char *ptail = zipEntryTail(zl);
        if (ptail[0] != ZIP_LIST_END) { // check ziplist is empty
            prevlen = zipRawEntryLength(ptail);
        }
    }


    // try strconv string -> long long, encoding changed when success.
    if (zipTryEncoding(s, slen, &encoding, &value)) {
        reqlen = zipIntSize(encoding);
    } else {
        reqlen = slen;
    }

    reqlen += zipStoreEntryEncoding(NULL, encoding, slen);
    reqlen += zipStoreEntryPrevLen(NULL, prevlen);

    nextdiff = p[0] != ZIP_LIST_END ? zipEntryPrevlenBytesDiff(p, reqlen) : 0;

    if (nextdiff == -4 && reqlen < 4) {
        nextdiff = 0;
        forcelarge = 1;
    }

    offset = p - zl;
    zl = ziplistResize(zl, curlen + reqlen + nextdiff);
    p = zl + offset;

    tailoffset = ziplistTailOffset(zl);

    // memmove [p, end] -> [p+reqlen, end]
    if (p[0] != ZIP_LIST_END) {

        memmove(p-nextdiff, p+reqlen, curlen - offset + nextdiff - 1);

        if (forcelarge) {
            zipStoreEntryPrevLenLarge(p+reqlen, reqlen);
        } else {
            // reset next entry -> prevlen
            zipStoreEntryPrevLen(p+reqlen, reqlen);
        }

        // store tail offset
        ziplistStoreTailOffset(zl, tailoffset + reqlen + nextdiff);

    } else {
        ziplistStoreTailOffset(zl, p - zl);
    }

    if (nextdiff != 0) {
        zl = __ziplistCascadeUpdate(zl, p+reqlen); // cascade update next entry prevlensize
        p = zl + offset;
    }

    uint32_t l =  zipStoreEntryPrevLen(p, prevlen);
    l += zipStoreEntryEncoding(p+l, encoding, slen);
    if (ZIP_IS_STR(encoding)) {
        memcpy(p+l, s, slen);
    } else {
        ziplistSaveInteger(p+l, encoding, value);
    }

    // incr length
    ziplistIncrLength(zl);
    return zl;
}

unsigned char *ziplistNew() {

    unsigned int size = HDRSIZE + 1;
    unsigned char* zl = zmalloc(size);
    ziplistStoreByteslen(zl, size);
    ziplistStoreTailOffset(zl, HDRSIZE);
    ziplistStoreLength(zl, 0);
    zl[size-1] = ZIP_LIST_END;
    return zl;
}

unsigned char *ziplistNext(unsigned char *zl, unsigned char *p) {

    if (p[0] == ZIP_LIST_END) return NULL;
    uint32_t rawlen= zipRawEntryLength(p);
    p += rawlen;
    if (p[0] == ZIP_LIST_END) return NULL;
    return p;
}

unsigned char *ziplistPrev(unsigned char *zl, unsigned char *p) {

  if (p[0] == ZIP_LIST_END) {
      p = zl + ziplistTailOffset(zl);
      return p[0] == ZIP_LIST_END ? NULL : p;
  } else if (p == zl+HDRSIZE) {
      return NULL;
  } else {
      int prevlensize;
      zipDecodeEntryPrevLen(zl, &prevlensize);
      return p - prevlensize;
  }
}

unsigned char *ziplistPush(unsigned char *zl, unsigned char *s, size_t slen, int where) {
    unsigned char *p;
    (where == ZIPLIST_INSERT_HEAD) ? (p = zl + HDRSIZE) : (p = zl + ziplistTailOffset(zl));
    return __ziplistInsert(zl, p, s, slen);
}

unsigned char *ziplistIndex(unsigned char *zl, int index) {

    int negative = (index >= 0) ? 0 : 1;
    uint32_t prevlen, rawlen;
    unsigned char *p = negative ? ziplistTail(zl): ziplistHeader(zl);

    if (p[0] == ZIP_LIST_END) {
        return NULL;
    }

    if (negative) {
        index = -index - 1;
        prevlen = zipDecodeEntryPrevLen(p, NULL);
        while (prevlen && index--) {
            p-=prevlen;
            prevlen = zipDecodeEntryPrevLen(p, NULL);
        }
        return index > 0 ? NULL : p;
    }

    while (p[0] != ZIP_LIST_END && index--) {
        rawlen = zipRawEntryLength(p);
        p+=rawlen;
    }

    return index > 0 ? NULL : p;

}

unsigned int ziplistGet(unsigned char *p, unsigned char **sstr, unsigned int *slen, long long *value) {

    unsigned char encoding;
    int prevlensize, rawlensize;
    if (p == NULL || p[0] == ZIP_LIST_END)
        return 0;

    if (sstr) *sstr = NULL;

    prevlensize = zipDecodePrevlensSize(p);
    encoding = zipTryDecodeEncoding(p+prevlensize);
    p+=prevlensize;
    if (ZIP_IS_STR(encoding)) {

        if (encoding == ZIP_STR_06B) {
            *slen = ~ZIP_STR_MASK & p[0];
            rawlensize = 1;
        } else if (encoding == ZIP_STR_14B) {
            *slen  = ~ZIP_STR_MASK & p[0] << 8 |
                    p[1];
            rawlensize = 2;
        } else {
            *slen  = p[1] << 24 |
                     p[2] << 16 |
                     p[3] << 8  |
                     p[4];
            rawlensize = 5;
        }

        *sstr = p+rawlensize;

    } else {

        ziplistLoadInteger(p, value);

    }

    return 1;

}