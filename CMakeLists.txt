cmake_minimum_required(VERSION 3.16)
project(redis C)

set(CMAKE_C_STANDARD 11)

aux_source_directory(${PROJECT_SOURCE_DIR}/src SRC_LIST)

add_executable(redis ${SRC_LIST})


