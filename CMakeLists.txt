cmake_minimum_required(VERSION 3.16)
project(redis C)

set(CMAKE_C_STANDARD 11)

add_executable(redis src/db.c src/dict.c src/dict.h src/server.c src/server.h src/sds.c src/sds.h src/varint.c src/varint.h src/object.c src/t_string.c src/utils.c src/utils.h src/evict.c src/zmalloc.h src/zmalloc.c)