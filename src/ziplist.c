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

void memrev32(void *ptr) {
    unsigned char *x=ptr, t;
    t = x[4];
    x[4] = x[0];
    x[0] = t;
    t = x[3];
    x[3] = x[2];
    x[2] = t;
}

uint32_t int32rev(uint32_t v) {
    memrev32(&v);
    return v;
}

uint32_t byteslen(unsigned char *zl) {
    uint32_t *ptr = (uint32_t*)(zl);
    return int32revifbe(*ptr);
}

uint32_t ziptailoffset(unsigned char *zl) {
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
   encoding = zipTryDecodeEncoding(p);
   if (ZIP_IS_STR(encoding)) {
       if (encoding == ZIP_STR_06B) {
           rawlensize = 1;
           rawlen = ~ZIP_STR_MASK & p[0];
       } else if (encoding == ZIP_STR_14B) {
           rawlensize = 2;
           rawlen = ~ZIP_STR_MASK & p[0] << 8 | p[1];
       } else {
           rawlensize = 5;
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

unsigned char *ziplistResize(unsigned char *zl, uint32_t newsize) {
    zrealloc(zl, newsize+1);
    zl[newsize] = ZIP_LIST_END;
    return zl;
}

void zipStoreTailOffset(unsigned char *zl, uint32_t tail) {
    tail = int32revifbe(tail);
    memcpy(zl, &tail, 4);
}

void zipStoreByteslen(unsigned char *zl, uint32_t byteslen) {
    byteslen = int32revifbe(byteslen);
    memcpy(zl, &byteslen, 4);
}

void zipSaveInteger(unsigned char *p, unsigned char encoding, long long value) {
    if (encoding == ZIP_INT_08B) {
        memcpy(p, &value, 1);
    } else if (encoding == ZIP_INT_16B) {
        memcpy(p, &value, 2);
    } else if (encoding == ZIP_INT_32B) {
        memcpy(p, &value, 4);
    } else {
        memcpy(p, &value, 8);
    }
}


unsigned char *__ziplistCascadeUpdate(unsigned char *zl, unsigned char *p) {

}


unsigned char *__ziplistInsert(unsigned char *zl, unsigned char *p, unsigned char *s, unsigned int slen) {

    uint32_t reqlen, offset, tailoffset, prevlen = 0, curlen = byteslen(zl);
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

    tailoffset = ziptailoffset(zl);

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
        zipStoreTailOffset(zl, reqlen + nextdiff);

    } else {
        zipStoreTailOffset(zl, curlen);
    }

    if (nextdiff != 0) {
        zl = __ziplistCascadeUpdate(zl, p+reqlen); // cascade update next entry prevlensize
        p = zl + offset;
    }

    uint32_t l =  zipStoreEntryPrevLen(p, prevlen);
    l += zipStoreEntryEncoding(p, encoding, slen);
    if (ZIP_IS_STR(encoding)) {
        memcpy(p+l, s, slen);
    } else {
        zipSaveInteger(p+l, encoding, value);
    }

    zipStoreByteslen(zl, curlen + reqlen + nextdiff);
    // incr length

    return zl;
}
