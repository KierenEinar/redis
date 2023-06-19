//
// Created by kieren jiang on 2023/6/18.
//

#ifndef REDIS_CRC16_H
#define REDIS_CRC16_H
#include <stdint.h>
#include <sys/types.h>

/**
 * name: xmodem
 * width: 16
 * poly: x16+x12+x5+x0, ignore the first msb is 0x1021 (00010000 00100001)
 * init: 0x0000
 * reflect input byte: false
 * reflect output CRC: false
 * xor constant to output CRC : 0x0000
 * */
uint16_t crc16(const char *buf, size_t len);

#endif //REDIS_CRC16_H
