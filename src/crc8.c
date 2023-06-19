//
// Created by kieren jiang on 2023/6/18.
//

#include "crc8.h"
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