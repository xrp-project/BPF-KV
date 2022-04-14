cmake_minimum_required(VERSION 3.15)
project(simple_kv C)

set(CMAKE_C_STANDARD 99)
set(CMAKE_C_FLAGS "-g -D_GNU_SOURCE")

add_executable(db db.c)
add_executable(db-bpf db-bpf.c uring.c)
target_link_libraries(db uring pthread m)
target_link_libraries(db-bpf bpf pthread m)
