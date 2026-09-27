cmake_minimum_required(VERSION 3.20)
project(distributed_kv_store LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

if(NOT CMAKE_BUILD_TYPE AND NOT CMAKE_CONFIGURATION_TYPES)
  set(CMAKE_BUILD_TYPE Release CACHE STRING "Build type" FORCE)
endif()

if(CMAKE_CXX_COMPILER_ID MATCHES "GNU|Clang")
  add_compile_options(-Wall -Wextra -Wpedantic)
endif()

# Link the runtime statically on MinGW so the binaries run without the
# toolchain's DLLs on PATH.
if(MINGW)
  add_link_options(-static)
endif()

find_package(Threads REQUIRED)

add_library(kvcore STATIC
  src/common/crc32.cpp
  src/common/hash.cpp
  src/common/file_util.cpp
  src/common/platform.cpp
  src/storage/block_cache.cpp
  src/storage/bloom_filter.cpp
  src/storage/memtable.cpp
  src/storage/wal.cpp
  src/storage/write_batch.cpp
  src/storage/sstable.cpp
  src/storage/lsm_tree.cpp
  src/raft/messages.cpp
  src/raft/raft_storage.cpp
  src/raft/raft.cpp
  src/net/sim_network.cpp
  src/net/socket.cpp
  src/net/tcp_transport.cpp
  src/kv/cluster_config.cpp
  src/kv/consistent_hash.cpp
  src/kv/kv_protocol.cpp
  src/kv/kv_state_machine.cpp
  src/kv/partition_replica.cpp
  src/kv/kv_node.cpp
  src/kv/kv_client.cpp
  src/kv/local_cluster.cpp
  src/check/linearizability.cpp
)
target_include_directories(kvcore PUBLIC src)
target_link_libraries(kvcore PUBLIC Threads::Threads)
if(WIN32)
  target_link_libraries(kvcore PUBLIC ws2_32 winmm)
endif()

add_executable(kvnode apps/kvnode.cpp)
target_link_libraries(kvnode PRIVATE kvcore)

add_executable(kvcli apps/kvcli.cpp)
target_link_libraries(kvcli PRIVATE kvcore)

add_executable(kvbench apps/kvbench.cpp)
target_link_libraries(kvbench PRIVATE kvcore)

add_executable(kvtests
  tests/main.cpp
  tests/storage_test.cpp
  tests/hash_ring_test.cpp
  tests/linearizability_test.cpp
  tests/raft_test.cpp
  tests/kv_test.cpp
  tests/tcp_test.cpp
)
target_link_libraries(kvtests PRIVATE kvcore)

enable_testing()
add_test(NAME storage COMMAND kvtests storage.)
add_test(NAME hash_ring COMMAND kvtests ring.)
add_test(NAME linearizability_checker COMMAND kvtests porcupine.)
add_test(NAME raft COMMAND kvtests raft.)
add_test(NAME kv COMMAND kvtests kv.)
add_test(NAME tcp COMMAND kvtests tcp.)
