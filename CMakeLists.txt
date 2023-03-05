cmake_minimum_required(VERSION 3.16)
project(redis C)

set(CMAKE_C_STANDARD 11)

add_executable(redis src/db.c src/dict.c src/dict.h src/redis.c src/redis.h src/sds.c src/sds.h src/varint.c src/varint.h)