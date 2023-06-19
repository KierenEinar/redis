//
// Created by kieren jiang on 2023/6/18.
//

#include "crc16.h"

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