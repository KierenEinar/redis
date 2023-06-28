//
// Created by kieren jiang on 2023/6/18.
//

#include "crc.h"

/**
 *
 * width: 8
 * poly: x8+x5+x4+x0, ignore the first msb is 0x31 (000110001)
 * init: 0x00
 * reflect input byte: false
 * reflect output CRC: False
 * xor constant to output CRC : 0x00
 * */
uint8_t crc8(const char *buf, size_t len) {

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

uint16_t crc16(const char *buf, size_t len) {
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

uint32_t crc32(const char *buf, size_t len) {
    uint32_t poly = 0x4C11DB7;
    uint32_t reg = 0x00;
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