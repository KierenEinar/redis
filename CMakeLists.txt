cmake_minimum_required(VERSION 3.16)
project(redis C)

set(CMAKE_C_STANDARD 11)

add_executable(redis main.c ziplist.h ziplist.c zskiplist.c zskiplist.h sds.c sds.h)