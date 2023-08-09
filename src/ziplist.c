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
#include <stdio.h>

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

uint16_t ziplistEntryNums(unsigned char *zl) {
    uint16_t entries;
    memcpy(&entries, zl+8, 2);
    memrev16ifbe(entries);
    return entries;
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

unsigned char zipTryDecodeEncoding(unsigned char *p) {
    if ((p[0] & ZIP_STR_MASK) < ZIP_STR_MASK) {
        return p[0] & ZIP_STR_MASK;
    }
    return p[0];
}

uint32_t zipRawEntryLength(unsigned char *p, int *prevlensize, int *rawlensize, uint32_t *rawlen, unsigned char *encoding) {

   // encoding str: prevlensize + encoding + rawlensize(exists when rawlen > 63) + rawlen

   // encoding int: prevlensize + encoding + intxxbit_byte

   uint32_t _rawlen;
   unsigned char _encoding;
   int _prevlensize, _rawlensize;
   zipDecodeEntryPrevLen(p, &_prevlensize);
    _encoding = zipTryDecodeEncoding(p+_prevlensize);

   if (ZIP_IS_STR(_encoding)) {
       if (_encoding == ZIP_STR_06B) {
           _rawlensize = 1;
           p = p + _prevlensize;
           _rawlen = ~ZIP_STR_MASK & p[0];
       } else if (_encoding == ZIP_STR_14B) {
           _rawlensize = 2;
           p = p + _prevlensize;
           _rawlen = (((~ZIP_STR_MASK) & p[0]) << 8) | p[1];
       } else {
           _rawlensize = 5;
           p = p + _prevlensize;
           _rawlen = p[1] << 24 | p[2] << 16 | p[3] << 8 | p[4];
       }
   } else {
       _rawlensize = 1;
       if (_encoding == ZIP_INT_08B) {
           _rawlen = 1;
       } else if (_encoding == ZIP_INT_16B) {
           _rawlen = 2;
       } else if (_encoding == ZIP_INT_32B) {
           _rawlen = 4;
       } else {
           _rawlen = 8;
       }
   }

   if (prevlensize) *prevlensize = _prevlensize;
   if (rawlensize) *rawlensize = _rawlensize;
   if (rawlen) *rawlen = _rawlen;
   if (encoding) *encoding = _encoding;

   return _prevlensize + _rawlensize + _rawlen;
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
    if (length > ZIPLIST_LENGTH_MAX)
        length = ZIPLIST_LENGTH_MAX;

    length = int16revifbe(length);
    memcpy(zl+8, &length, 2);
}

unsigned char *ziplistResize(unsigned char *zl, uint32_t newsize) {
    zl = zrealloc(zl, newsize);
    zl[newsize-1] = ZIP_LIST_END;
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
        memrev16ifbe(&i16);
        memcpy(p, &i16, 2);
    } else if (encoding == ZIP_INT_32B) {
        i32 = (int32_t)value;
        memrev32ifbe(&i32);
        memcpy(p, &i32, 4);

    } else {
        memrev64ifbe(&value);
        memcpy(p, &value, 8);
    }
}

void ziplistLoadInteger(unsigned char *p, int prevlensize, unsigned char encoding, long long *value) {

    int8_t i8;
    int16_t i16;
    int32_t i32;
    int64_t i64;

    if (encoding == ZIP_INT_08B) {
        memcpy(&i8, p, 1);
        *value = (long long)(i8);
    } else if (encoding == ZIP_INT_16B) {
        memcpy(&i16, p, 2);
        memrev16ifbe(&i16);
        *value = (long long)(i16);
    } else if (encoding == ZIP_INT_32B) {
        memcpy(&i32, p, 4);
        memrev32ifbe(&i32);
        *value = (long long)(i32);
    } else {
        memcpy(&i64, p, 8);
        memrev64ifbe(&i64);
        *value = (long long)(i64);
    }

}

void ziplistIncrLength(unsigned char *zl, int incrlen) {

    uint16_t length;
    memcpy(&length, zl+8, 2);
    length = int16revifbe(length);
    if (length + incrlen < 0xffff) {
        length = int16revifbe(length+incrlen);
        memcpy(zl+8, &length, 2);
    }
}

unsigned char *ziplistHeader(unsigned char *zl) {
    return zl + ZIPLIST_HEADER;
}

unsigned char *ziplistTail(unsigned char *zl) {
    return zl + ziplistTailOffset(zl);
}


unsigned char *__ziplistCascadeUpdate(unsigned char *zl, unsigned char *p) {

    uint32_t rawlen, prevlen, rawlensize, curlen, offset;

    int nextdiff, prevlensize;

    curlen = ziplistBytesLen(zl);

    while (p[0] != ZIP_LIST_END) {

        rawlen = zipRawEntryLength(p, NULL, NULL, NULL, NULL);
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
                zipStoreEntryPrevLenLarge(p+rawlen, rawlen);
            } else {
                zipStoreEntryPrevLen(p+rawlen, rawlen);
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
        unsigned char *ptail = ziplistTail(zl);
        if (ptail[0] != ZIP_LIST_END) { // check ziplist is empty
            prevlen = zipRawEntryLength(ptail, NULL, NULL, NULL, &encoding);
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

        memmove(p+reqlen, p-nextdiff, curlen - offset + nextdiff - 1);

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
    ziplistIncrLength(zl, 1);
    return zl;
}

unsigned char *ziplistInsert(unsigned char *zl, unsigned char *p, unsigned char *s, unsigned int slen) {
    return __ziplistInsert(zl, p, s, slen);
}

unsigned char *__ziplistDelete(unsigned char *zl, unsigned char *p, unsigned int num) {
    unsigned int j=0;
    uint32_t rawlen, totlen, firstprevlen, curlen, tailoffset, offset;
    unsigned char *first;
    int firstprevlensize, deleted=0, nextdff = 0;
    first = p;
    curlen = ziplistBytesLen(zl);
    for (j=0; p[0]!=ZIP_LIST_END && j<num; j++) {
        rawlen = zipRawEntryLength(p, NULL, NULL, NULL, NULL);
        p+=rawlen;
        deleted++;
    }

    totlen = p - first;
    if (totlen > 0) {

        if (p[0] != ZIP_LIST_END) {

            firstprevlen = zipDecodeEntryPrevLen(first, &firstprevlensize);
            nextdff = zipEntryPrevlenBytesDiff(p, firstprevlen);
            p-=nextdff;
            zipStoreEntryPrevLen(p, firstprevlen);
            memmove(first, p, curlen-(p-zl));
            tailoffset = ziplistTailOffset(zl);
            ziplistStoreTailOffset(zl, tailoffset - totlen + nextdff);

            p = first;

        } else {

            unsigned char *prev = ziplistPrev(zl, first);
            if (prev) {
                ziplistStoreTailOffset(zl, prev-zl);
            } else {
                ziplistStoreTailOffset(zl, ZIPLIST_HEADER);
            }
        }

        offset = p - zl;

        zl = ziplistResize(zl, curlen - totlen + nextdff);

        p = zl + offset;

        if (nextdff != 0) {
            zl = __ziplistCascadeUpdate(zl, p);
            p = zl + offset;
        }

        ziplistIncrLength(zl, -deleted);

    }

    return zl;

}


unsigned char *ziplistNew() {

    unsigned int size = ZIPLIST_HEADER + 1;
    unsigned char* zl = zmalloc(size);
    ziplistStoreByteslen(zl, size);
    ziplistStoreTailOffset(zl, ZIPLIST_HEADER);
    ziplistStoreLength(zl, 0);
    zl[size-1] = ZIP_LIST_END;
    return zl;
}

unsigned char *ziplistNext(unsigned char *zl, unsigned char *p) {

    if (p[0] == ZIP_LIST_END) return NULL;
    uint32_t rawlen= zipRawEntryLength(p, NULL, NULL, NULL, NULL);
    p += rawlen;
    if (p[0] == ZIP_LIST_END) return NULL;
    return p;
}

unsigned char *ziplistPrev(unsigned char *zl, unsigned char *p) {

  if (p[0] == ZIP_LIST_END) {
      p = zl + ziplistTailOffset(zl);
      return p[0] == ZIP_LIST_END ? NULL : p;
  } else if (p == zl + ZIPLIST_HEADER) {
      return NULL;
  } else {
      uint32_t prevlen;
      prevlen = zipDecodeEntryPrevLen(p, NULL);
      return p - prevlen;
  }
}

unsigned char *ziplistMerge(unsigned char **first, unsigned char **second) {

    if (first == NULL || *first == NULL || second == NULL || *second == NULL) {
        return NULL;
    }

    if (*first == *second) {
        return NULL;
    }

    unsigned int firstByteslen = ziplistBytesLen(*first);
    unsigned int secondByteslen = ziplistBytesLen(*second);

    unsigned char *target,*source;
    unsigned int targetByteslen, sourceByteslen, length, firstOffset;
    int append;

    if (firstByteslen >= secondByteslen) {
        target = *first;
        source = *second;
        targetByteslen = firstByteslen;
        sourceByteslen = secondByteslen;
        append = 1;
    } else {
        target = *second;
        source = *first;
        targetByteslen = secondByteslen;
        sourceByteslen = firstByteslen;
        append = 0;
    }

    firstOffset = ziplistTailOffset(*first);
    target = ziplistResize(target, firstByteslen - ZIPLIST_END_SIZE + secondByteslen - ZIPLIST_HEADER);
    length = ziplistBloblen(*first) + ziplistBloblen(*second);
    ziplistStoreLength(target, length);
    ziplistStoreTailOffset(target, firstByteslen - ZIPLIST_END_SIZE + ziplistTailOffset(*second) - ZIPLIST_HEADER);

    if (append) {
        memcpy(target+firstByteslen-ZIP_LIST_END, source+ZIPLIST_HEADER, secondByteslen - ZIPLIST_HEADER);
    } else {
        memmove(target+sourceByteslen-ZIP_LIST_END, target+ZIPLIST_HEADER, targetByteslen - ZIPLIST_HEADER);
        memcpy(target+ZIPLIST_HEADER, source+ZIPLIST_HEADER, sourceByteslen-ZIPLIST_HEADER-1);
    }

    target = __ziplistCascadeUpdate(target, target+ firstOffset);

    if (*first == source) {
        zfree(*first);
        *first = NULL;
    }

    if (*second == source) {
        zfree(*second);
        *second = NULL;
    }

    return target;

}

unsigned char *ziplistPush(unsigned char *zl, unsigned char *s, size_t slen, int where) {
    unsigned char *p;
    (where == ZIPLIST_INSERT_HEAD) ? (p = zl + ZIPLIST_HEADER) : (p = zl + ziplistBytesLen(zl) - 1);
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
        }
        return index > 0 ? NULL : p;
    }

    while (p[0] != ZIP_LIST_END && index--) {
        rawlen = zipRawEntryLength(p, NULL, NULL, NULL, NULL);
        p+=rawlen;
    }

    return index > 0 ? NULL : p;

}

unsigned int ziplistGet(unsigned char *p, unsigned char **sstr, unsigned int *slen, long long *value) {

    unsigned char encoding;
    int prevlensize, rawlensize;
    unsigned int _slen;
    long long _value;
    if (p == NULL || p[0] == ZIP_LIST_END)
        return 0;

    if (sstr) *sstr = NULL;

    prevlensize = zipDecodePrevlensSize(p);
    encoding = zipTryDecodeEncoding(p+prevlensize);
    p+=prevlensize;
    if (ZIP_IS_STR(encoding)) {

        if (encoding == ZIP_STR_06B) {
            _slen = ~ZIP_STR_MASK & p[0];
            rawlensize = 1;
        } else if (encoding == ZIP_STR_14B) {
            _slen  = (((~ZIP_STR_MASK) & p[0]) << 8) |
                    p[1];
            rawlensize = 2;
        } else {
            _slen  = p[1] << 24 |
                     p[2] << 16 |
                     p[3] << 8  |
                     p[4];
            rawlensize = 5;
        }

        *sstr = p+rawlensize;

        if (slen) *slen = _slen;

    } else {

        ziplistLoadInteger(p+1, prevlensize, encoding, &_value);
        if (value) *value = _value;

    }

    return 1;

}

void ziplistRepr(unsigned char *zl) {

    unsigned char *p, encoding;
    int index = 0, hdrlen, prevlensize, rawlensize;
    uint32_t prevlen, rawlen, entrylen;
    long long value;
    printf(
            "{total bytes: %u}\n"
            "{num entries: %d}\n"
            "{tails offset: %u}\n",
            ziplistBytesLen(zl),
            ziplistEntryNums(zl),
            ziplistTailOffset(zl)
            );

    p = zl + ZIPLIST_HEADER;

    while (p[0] != ZIP_LIST_END) {

        entrylen = zipRawEntryLength(p, &prevlensize, &rawlensize, &rawlen, &encoding);

        prevlen = zipDecodeEntryPrevLen(p, NULL);
        hdrlen = prevlensize + rawlensize;

        printf(
                "{\n"
                "\taddr: 0x%08lx,\n"
                "\tindex: %2d,\n"
                "\toffset: %5ld,\n"
                "\thdrlen: %2d,\n" // prevlensize + rawlensize(including encoding)
                "\tprevlen: %5d,\n"  //
                "\tprevlensize: %2d,\n"
                "\trawlensize: %2d,\n"
                "\tpayloadlen: %u,\n",
                (unsigned long)p,
                index,
                p-zl,
                hdrlen,
                prevlen,
                prevlensize,
                rawlensize,
                rawlen
                );

        p += hdrlen;

        if (ZIP_IS_STR(encoding)) {
            printf("\t[str]:");
            if (rawlen > 40) {
                if (fwrite(p, 40, 1, stdout) == 0) perror("fwrite");
                printf("...");
            } else {
                if (rawlen && fwrite(p, rawlen, 1, stdout) == 0) perror("fwrite");
            }

        } else {
            printf("");
            ziplistLoadInteger(p, prevlensize, encoding, &value);
            printf("\t[int]:%lld", value);
        }
        printf("\n}\n");

        index++;
        p+=rawlen;
    }

}

unsigned char *ziplistFind(unsigned char *p, unsigned char *str, unsigned int slen, unsigned int skipcnt) {

    unsigned int skip = 0;
    unsigned char encoding, vencoding = 0;
    uint32_t rawlen, entrylen;
    int prevlensize, rawlensize;
    long long vv, value;
    while (p[0] != ZIP_LIST_END) {

        entrylen = zipRawEntryLength(p, &prevlensize, &rawlensize, &rawlen, &encoding);

        if (skip == 0) {

            encoding = zipTryDecodeEncoding(p+prevlensize);
            if (ZIP_IS_STR(encoding)) {
                if (slen == rawlen && memcmp(p+prevlensize+rawlensize, str, rawlen) == 0) {
                    return p;
                }
            } else {

                if (vencoding == 0) {
                    if (!zipTryEncoding(str, slen, &vencoding, &vv)) {
                        vencoding = UINT8_MAX;
                    }
                }

                // str convert to long long success.
                if (vencoding != UINT8_MAX) {
                    ziplistLoadInteger(p+prevlensize+rawlensize, prevlensize, encoding, &value);
                    if (value == vv) {
                        return p;
                    }
                }
            }

            skip = skipcnt;
        } else {
            skip--;
        }

        p = p+entrylen;

    }

    return NULL;
}

unsigned char *ziplistDeleteRange(unsigned char *zl, int index, unsigned int num) {

    unsigned  char *p = ziplistIndex(zl, index);
    return p == NULL ? zl : __ziplistDelete(zl, p, num);
}

uint32_t ziplistBloblen(unsigned char *zl) {
   return ziplistBytesLen(zl);
}

uint32_t ziplistlen(unsigned char *zl) {
    uint16_t len;
    memcpy(&len, zl + 8, 2);
    memrev16ifbe(&len);
    if (len < ZIPLIST_LENGTH_MAX)
        return (uint32_t)len;

    uint32_t idx = 0;
    zl = zl + ZIPLIST_HEADER;
    while (zl[0] != ZIP_LIST_END) {
        zl += zipRawEntryLength(zl, NULL, NULL, NULL, NULL);
        idx++;
    }
    return idx;
}

unsigned char *ziplistdup(unsigned char *zl) {
    if (!zl) return NULL;
    unsigned int byteslen = ziplistBytesLen(zl);
    unsigned char *nzl = zmalloc(byteslen);
    memcpy(nzl, zl, byteslen);
    return nzl;
}

void testZiplist() {

    int ix;
    char s[16385];
    unsigned char *zl = ziplistNew();
    // push elements with slen lte 63bytes
    memset(s, '1', 63);
    s[63] = '\0';
    ix = 3;
    while (ix--) {
        // push strlen 250 bytes, entry raw len will be 253bytes, every entry prevlensize will be 1 byte.
        zl = ziplistPush(zl, (unsigned char*)s, strlen(s), ZIPLIST_INSERT_TAIL);
    }

    // -------- push elements with slen gt 63bytes and lte 16383bytes  --------
    ix = 3;
    memset(s, '1', sizeof(s)-1);
    s[250] = '\0';
    while (ix--) {
        // push strlen 250 bytes, entry raw len will be 253bytes, every entry prevlensize will be 1 byte.
        zl = ziplistPush(zl, (unsigned char*)s, strlen(s), ZIPLIST_INSERT_TAIL);
    }

    // -------- push elements with slen gt 16383bytes   --------
    ix = 3;
    memset(s, '1', sizeof(s)-1);
    s[16384] = '\0';
    while (ix--) {
        // push strlen 250 bytes, entry raw len will be 253bytes, every entry prevlensize will be 1 byte.
        zl = ziplistPush(zl, (unsigned char*)s, strlen(s), ZIPLIST_INSERT_TAIL);
    }

    // ------- ziplist get by index --------

//    prevlensize 		rawlensize (encoding | rawlen) 			raw
//    0		1byte(data:0)		01111111 (1bytes)						63bytes
//    1		1byte(data:65)		01111111 (1bytes)						63bytes
//    2		1byte(data:65)		01111111 (1bytes)						63bytes
//
//    3		1byte(data:65)		01000000 10010110(2bytes)				250bytes
//    4		1byte(data:253)	    01000000 10010110(2bytes)		        250bytes
//    5		1byte(data:253)	    01000000 10010110(2bytes)		        250bytes
//
//    6		1byte(data:253)	    01000000 pppppppp(5bytes)		        16384bytes
//    7	    5byte(data:16390)	01000000 pppppppp(5bytes)		        16384bytes
//    8		5byte(data:16390)	01000000 pppppppp(5bytes)		        16384bytes


    unsigned char * p = ziplistIndex(zl, 4);
    unsigned char *sstr;
    unsigned int slen;
    long long value;

    // ---------- ziplist index ---------------


    ziplistGet(p, &sstr, &slen, &value);

    memcpy(s, sstr, slen);
    s[slen] = '\0';
    printf("ziplist str get data[%s], len[%u]\n", s, slen);


    // ---------__ziplistInsert -----------

//    idx   prevlensize 		rawlensize (encoding | rawlen) 			raw             offset
//    0		1byte(data:0)		01111111 (1bytes)						63bytes         10
//    1		1byte(data:65)		01111111 (1bytes)						63bytes         75
//    2		1byte(data:65)		01111111 (1bytes)						63bytes         140
//
//    3		1byte(data:65)		01000000 10010110(2bytes)				250bytes        205
//    4     1byte(data:253)	    01000000 pppppppp(2bytes)               254bytes        458         (inserted)
//    5		5byte(data:260)	    01000000 10010110(2bytes)		        250bytes        715
//    6		5byte(data:257)	    01000000 10010110(2bytes)		        250bytes        972
//
//    7		5byte(data:257)	    10000000 pppppppp(5bytes)		        16384bytes      1229
//    8	    5byte(data:16394)	10000000 pppppppp(5bytes)		        16384bytes      17623
//    9		5byte(data:16394)	10000000 pppppppp(5bytes)		        16384bytes      34017

    // test cause cascade update
    memset(s, '2', 255);
    s[254] = '\0';

    zl = __ziplistInsert(zl, p, (unsigned char*)s, strlen(s));

    // test update large prevlen

//    idx   prevlensize 		rawlensize (encoding | rawlen) 			raw             offset
//    0		1byte(data:0)		01111111 (1bytes)						63bytes         10
//    1		1byte(data:65)		01111111 (1bytes)						63bytes         75
//    2		1byte(data:65)		01111111 (1bytes)						63bytes         140
//
//    3		1byte(data:65)		01000000 10010110(2bytes)				250bytes        205
//    4     1byte(data:253)	    01000000 pppppppp(2bytes)               254bytes        458
//    5     5byte(data:257)	    11000100         (1bytes)               8bytes          472           (inserted)
//    6		5byte(data:14)	    01000000 10010110(2bytes)		        250bytes        729
//    7		5byte(data:257)	    01000000 10010110(2bytes)		        250bytes        986
//
//    8		5byte(data:257)	    10000000 pppppppp(5bytes)		        16384bytes      1243
//    9	    5byte(data:16394)	10000000 pppppppp(5bytes)		        16384bytes      17637
//    10	5byte(data:16394)	10000000 pppppppp(5bytes)		        16384bytes      34031

    memset(s, '3', 10);
    s[10] = '\0';
    p = ziplistIndex(zl, 5);
    zl = __ziplistInsert(zl, p, (unsigned char*)s, strlen(s));

    // test delete index

//    idx   prevlensize 		rawlensize (encoding | rawlen) 			raw             offset
//    0		1byte(data:0)		01111111 (1bytes)						63bytes         10
//    1		1byte(data:65)		01111111 (1bytes)						63bytes         75
//    2		1byte(data:65)		01111111 (1bytes)						63bytes         140
//
//    3		1byte(data:65)		01000000 10010110(2bytes)				250bytes        205
//    4     1byte(data:253)	    01000000 pppppppp(2bytes)               254bytes        458
//    5     5byte(data:257)	    11000100         (1bytes)               8bytes          472
//    6		5byte(data:14)	    01000000 10010110(2bytes)		        250bytes        729
//    7		5byte(data:257)	    01000000 10010110(2bytes)		        250bytes        986
//
//    8		5byte(data:257)	    10000000 pppppppp(5bytes)		        16384bytes      1243
//    9	    5byte(data:16394)	10000000 pppppppp(5bytes)		        16384bytes      17637          (deleted)
//    10	5byte(data:16394)	10000000 pppppppp(5bytes)		        16384bytes      34031

    zl = ziplistDeleteRange(zl, 9, 1);

    // test delete last
//    idx   prevlensize 		rawlensize (encoding | rawlen) 			raw             offset
//    0		1byte(data:0)		01111111 (1bytes)						63bytes         10
//    1		1byte(data:65)		01111111 (1bytes)						63bytes         75
//    2		1byte(data:65)		01111111 (1bytes)						63bytes         140
//
//    3		1byte(data:65)		01000000 10010110(2bytes)				250bytes        205
//    4     1byte(data:253)	    01000000 pppppppp(2bytes)               254bytes        458
//    5     5byte(data:257)	    11000100         (1bytes)               8bytes          472
//    6		5byte(data:14)	    01000000 10010110(2bytes)		        250bytes        729
//    7		5byte(data:257)	    01000000 10010110(2bytes)		        250bytes        986
//
//    8		5byte(data:257)	    10000000 pppppppp(5bytes)		        16384bytes      1243
//    9	    5byte(data:16394)	10000000 pppppppp(5bytes)		        16384bytes      17637           (deleted)

    zl = ziplistDeleteRange(zl, 9, 1);

    ziplistRepr(zl);

}
