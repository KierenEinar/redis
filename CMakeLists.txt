cmake_minimum_required(VERSION 3.16)
project(redis C)

set(CMAKE_C_STANDARD 11)

add_executable(redis src/zmalloc.c src/adlist.c src/bio.c src/config.c src/el.c src/rio.c src/sds.c src/server.c
        src/syncio.c src/utils.c src/varint.c src/anet.c src/networking.c src/crc.c src/dict.c src/object.c src/t_string.c src/db.c src/expire.c
        src/ziplist.c)


