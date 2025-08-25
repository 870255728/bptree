cmake_minimum_required(VERSION 3.28.3)
project(bptree)

set(CMAKE_CXX_STANDARD 23)

find_package(Threads REQUIRED)

include_directories(./include/bptree DIR_INC_BP)
include_directories(./include/buffer DIR_INC_BU)
include_directories(./include/disk DIR_INC_DI)
include_directories(./include/common DIR_INC_CO)


include(FetchContent)
FetchContent_Declare(
        googletest
        URL https://github.com/google/googletest/archive/03597a01ee50ed33e9dfd640b249b4be3799d395.zip
)
# 使得 GoogleTest 可用
FetchContent_MakeAvailable(googletest)

aux_source_directory(./src/bptree DIR_SRCS_BPTREE)
aux_source_directory(./src/buffer DIR_SRCS_BUFFER)
aux_source_directory(./src/disk DIR_SRCS_DISK)
aux_source_directory(./test DIR_TEST)
aux_source_directory(./benchmark DIR_BEN)
aux_source_directory(./src/common DIR_SRCS_COMMON)

add_executable(bptree ${DIR_SRCS_BPTREE} ${DIR_SRCS_BUFFER} ${DIR_SRCS_DISK} ${DIR_TEST} ${DIR_BEN} ${DIR_SRCS_COMMON})

target_link_libraries(bptree gtest gtest_main Threads::Threads)