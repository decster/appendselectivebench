cmake_minimum_required(VERSION 3.21)
project(appendselectivebench)

set(CMAKE_CXX_STANDARD 11)
set(BENCHMARK_DOWNLOAD_DEPENDENCIES ON)

add_subdirectory(benchmark)

add_executable(appendselectivebench main.cpp)

target_link_libraries(appendselectivebench benchmark::benchmark)
