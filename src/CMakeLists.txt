cmake_minimum_required(VERSION 3.3)
project(HashDB)

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++11")

set(SOURCE_FILES main.c bloom.c hashdb.c bloom.h hashdb.h)
add_executable(HashDB ${SOURCE_FILES})