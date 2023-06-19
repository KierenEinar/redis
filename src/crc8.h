//
// Created by kieren jiang on 2023/6/18.
//

#ifndef REDIS_CRC8_H
#define REDIS_CRC8_H
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
#endif //REDIS_CRC8_H
