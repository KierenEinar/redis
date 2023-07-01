//
// Created by kieren jiang on 2023/6/18.
//

#include "crc.h"
#include "utils.h"

void fastLower(uint8_t *c) {
    if (*c >= 'A' && *c <= 'Z') {
        *c += 32;
    }
}

/**
 *
 * width: 8
 * poly: x8+x5+x4+x0, ignore the first msb is 0x31 (000110001)
 * init: 0x00
 * reflect input byte: false
 * reflect output CRC: False
 * xor constant to output CRC : 0x00
 * */
uint8_t crc8(const unsigned char *buf, size_t len) {

    uint8_t crc, i, poly;
    crc = 0x00;
    poly = 0x31;
    while (len--) {
        crc ^= *(buf++);
        for (i=0; i<8; i++) {
            if (crc & 0x80) {
                crc = (crc << 1) ^ poly;
            } else {
                crc <<=1;
            }
        }
    }
    return crc;
}

uint16_t crc16(const unsigned char *buf, size_t len) {
    uint16_t crc, poly;
    uint8_t i, byte;

    crc = 0x0000;
    poly = 0x1021;
    while (len--) {
        byte = (*buf++);
        crc ^= (byte << 8);
        for (i=0; i<8; i++) {
            if (crc & 0x8000) {
                crc = (crc << 1) ^ poly;
            } else {
                crc <<= 1;
            }
        }
    }

    return crc;
}

uint32_t crc32(const unsigned char *buf, size_t len) {
    uint32_t poly = 0x4C11DB7;
    uint32_t reg =  0xFFFFFFFF;
    uint8_t byte;
    short j;
    while (len--) {
        byte = (*buf++);
        reg ^= (byte << 24);

        for (j=0; j<8; j++) {
            if (reg & 0x80000000) {
                reg = (reg << 1) ^ poly;
            } else {
                reg <<= 1;
            }
        }
    }
    return reg;
}

uint64_t crc64_case(const unsigned char *buf, size_t len, int ignore_case) {

    uint8_t byte, j;
    uint64_t refIn, xorOut = 0xFFFFFFFFFFFFFFFF, poly = 0x42F0E1EBA9EA3693, crcReg = 0xFFFFFFFFFFFFFFFF;

    while (len--) {
        byte = (*buf++);
        if (ignore_case) {
            fastLower(&byte);
        }
        crcReg ^= uu_rev(byte); // 8bit rev including left shift 56 bits.
        for (j=0; j<8; j++) {
            if (crcReg & 0x8000000000000000) {
                crcReg = (crcReg << 1) ^ poly;
            } else {
                crcReg <<= 1;
            }
        }
    }

    crcReg = uu_rev(crcReg);
    crcReg ^= xorOut;
    return crcReg;
}

uint64_t crc64(const unsigned char *buf, size_t len) {
   return crc64_case(buf, len, 0);
}

uint64_t crc64_nocase(const unsigned char *buf, size_t len) {
    return crc64_case(buf, len, 1);
}
