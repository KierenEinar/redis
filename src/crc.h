//
// Created by kieren jiang on 2023/6/18.
//

#ifndef REDIS_CRC_H
#define REDIS_CRC_H
#include <stdint.h>
#include <sys/types.h>

/**
 *
 * width: 8
 * poly: x8+x5+x4+x0, ignore the first msb is 0x31 (000110001)
 * init: 0x00
 * reflect input byte: false
 * reflect output CRC: false
 * xor constant to output CRC : 0x00
 * */
uint8_t crc8(const char *buf, size_t len);

/**
 * name: crc-16/xmodem
 * width: 16
 * poly: x16+x12+x5+x0, ignore the first msb is 0x1021 (00010000 00100001)
 * init: 0x0000
 * reflect input byte: false
 * reflect output CRC: false
 * xor constant to output CRC : 0x0000
 * */
uint16_t crc16(const char *buf, size_t len);

/**
 * name: crc-32/mpeg-2
 * width: 32
 * poly: 0x4C11DB7
 * init: 0xFFFFFFFF
 * reflect input byte: false
 * reflect output CRC: false
 * xor constant to output CRC : 0x0000
 * input: hello world
 * output hex: BB08EC87
 * */
uint32_t crc32(const char *buf, size_t len);

#endif //REDIS_CRC_H
