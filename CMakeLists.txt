cmake_minimum_required(VERSION 3.14)
project(cloudlab
        VERSION 0.1
        DESCRIPTION "TUM Cloud Computing Lab"
        LANGUAGES CXX)

include(FetchContent)

set(CMAKE_BUILD_TYPE Debug)
set(CMAKE_CXX_STANDARD 20)
set(CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH} "${CMAKE_SOURCE_DIR}/cmake/modules/")
# set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -fsanitize=address -fno-omit-frame-pointer")
# set(CMAKE_CXX_FLAGS_COVERAGE "${CMAKE_CXX_FLAGS_COVERAGE} ${CMAKE_CXX_FLAGS_DEBUG} -fprofile-instr-generate -fcoverage-mapping")
add_compile_options(-Wall -Wextra -pedantic -Wformat=2 -g -Wno-unused -pthread)


#detect dependencies
find_package(Threads REQUIRED)
find_package(GTest REQUIRED)
find_package(RocksDB REQUIRED)
find_package(Protobuf REQUIRED)
find_package(LibEvent REQUIRED)

find_library(CLOUDLAB_LIBEVENT event REQUIRED)
if (CLOUDLAB_LIBEVENT)
    message(STATUS "Found libevent: " ${CLOUDLAB_LIBEVENT})
else ()
    message(FATAL_ERROR "libevent not found")
endif ()

find_library(CLOUDLAB_LIBEVENT_PTHREADS event_pthreads REQUIRED)
if (CLOUDLAB_LIBEVENT_PTHREADS)
    message(STATUS "Found libevent_pthreads: " ${CLOUDLAB_LIBEVENT_PTHREADS})
else ()
    message(FATAL_ERROR "libevent_pthreads not found")
endif ()

FetchContent_Declare(fmt
        GIT_REPOSITORY https://github.com/fmtlib/fmt.git
        GIT_TAG 9.0.0)

FetchContent_MakeAvailable(fmt)

# compile protobuf messages
protobuf_generate_cpp(PROTO_SRC PROTO_HDR ${PROJECT_SOURCE_DIR}/lib/message/cloud.proto)

# cloudlab library
add_library(cloudlab
        include/cloudlab/handler/p2p.hh
        include/cloudlab/handler/handler.hh
        include/cloudlab/network/server.hh
        include/cloudlab/handler/api.hh
        include/cloudlab/network/address.hh
        include/cloudlab/network/connection.hh
        include/cloudlab/message/message_helper.hh
        include/cloudlab/spmc.hh
        include/cloudlab/raft/raft.hh
        include/cloudlab/tx/tx.hh
        include/cloudlab/parser.hh
        include/cloudlab/argh.hh
        lib/handler/api.cc
        lib/network/server.cc
        lib/kvs.cc include/cloudlab/kvs.hh
        lib/handler/p2p.cc
        lib/network/address.cc
        lib/network/connection.cc
        lib/message/message_helper.cc
        lib/raft/raft.cc
        lib/tx/tx.cc
        lib/parser.cc
        ${PROTO_SRC}
        ${PROTO_HDR})
target_include_directories(cloudlab
        PUBLIC ${PROJECT_SOURCE_DIR}/include
        PRIVATE ${PROJECT_SOURCE_DIR}/lib
        PUBLIC ${CMAKE_CURRENT_BINARY_DIR}
        PUBLIC ${PROTOBUF_INCLUDE_DIR}
        PRIVATE ${LIBEVENT_INCLUDE_DIR})
target_link_libraries(cloudlab
        PUBLIC ${PROTOBUF_LIBRARY}
        PRIVATE fmt::fmt
        PRIVATE ${ROCKSDB_LIBRARY}
        PRIVATE Threads::Threads
        PRIVATE ${LIBEVENT_LIBRARY}
        PRIVATE ${CLOUDLAB_LIBEVENT}
        PRIVATE ${CLOUDLAB_LIBEVENT_PTHREADS})

# ctl executable
add_executable(ctl-test src/ctl.cc include/cloudlab/argh.hh)
target_link_libraries(ctl-test cloudlab fmt::fmt)

# kvs executable
add_executable(kvs-test src/kvs.cc include/cloudlab/argh.hh)
target_link_libraries(kvs-test cloudlab fmt::fmt)

# tests_clt
add_executable(tests_clt src/tests_clt.cc include/cloudlab/argh.hh)
target_link_libraries(tests_clt cloudlab fmt::fmt)