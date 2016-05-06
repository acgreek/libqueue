cmake_minimum_required (VERSION 2.6)
project (libqueue)
add_library(queue queue.c)

add_executable(qpush qpush.c) 
target_link_libraries(qpush queue)
