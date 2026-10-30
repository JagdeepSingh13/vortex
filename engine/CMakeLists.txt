cmake_minimum_required(VERSION 3.24)

project(exchange LANGUAGES CXX)

include(FetchContent)

find_package(Threads REQUIRED)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

add_library(cpp_exchange INTERFACE)

target_include_directories(
    cpp_exchange
    INTERFACE
        "${CMAKE_CURRENT_SOURCE_DIR}/src"
)

target_compile_features(cpp_exchange INTERFACE cxx_std_20)

target_compile_options(
    cpp_exchange
    INTERFACE
        -Wall
        -Wextra
        -Wpedantic
        -fno-exceptions
)

# ---- build options ----
# VORTEX_SANITIZE: "address,undefined" or "thread" (separate build dirs, see Makefile)
set(VORTEX_SANITIZE "" CACHE STRING "Sanitizers to enable, e.g. address,undefined or thread")
option(VORTEX_WERROR "Treat warnings as errors" ON)

if(VORTEX_WERROR)
    target_compile_options(cpp_exchange INTERFACE -Werror)
endif()

if(VORTEX_SANITIZE)
    # -fno-sanitize-recover: any finding fails the test instead of just printing
    target_compile_options(cpp_exchange INTERFACE
        -fsanitize=${VORTEX_SANITIZE} -fno-sanitize-recover=all -fno-omit-frame-pointer)
    target_link_options(cpp_exchange INTERFACE -fsanitize=${VORTEX_SANITIZE})
endif()

# sockets: Winsock on Windows, nothing extra on Linux/macOS
add_library(cpp_exchange_net INTERFACE)
target_link_libraries(cpp_exchange_net INTERFACE cpp_exchange Threads::Threads)
if(WIN32)
    target_link_libraries(cpp_exchange_net INTERFACE ws2_32)
endif()


add_executable(
    exchange
    src/exchange_main.cpp
)

target_link_libraries(
    exchange
    PRIVATE
        cpp_exchange_net
)

set_target_properties(
    exchange
    PROPERTIES
        OUTPUT_NAME "exchange"
        RUNTIME_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/src"
)

add_executable(
    trader_terminal
    tools/trader_terminal.cpp
)

target_link_libraries(
    trader_terminal
    PRIVATE
        cpp_exchange_net
)

set_target_properties(
    trader_terminal
    PROPERTIES
        OUTPUT_NAME "trader_terminal"
        RUNTIME_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/tools"
)


add_executable(
    liquidity_bot
    tools/liquidity_bot.cpp
)

target_link_libraries(
    liquidity_bot
    PRIVATE
        cpp_exchange_net
)

set_target_properties(
    liquidity_bot
    PROPERTIES
        OUTPUT_NAME "liquidity_bot"
        RUNTIME_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/tools"
)

# in-process engine + bots, no networking
add_executable(
    simulation
    tools/simulation.cpp
)

target_link_libraries(
    simulation
    PRIVATE
        cpp_exchange
        Threads::Threads
)

set_target_properties(
    simulation
    PROPERTIES
        RUNTIME_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/tools"
)

# ---- benchmark ----
add_executable(
    latency_bench
    bench/latency_bench.cpp
)

target_link_libraries(
    latency_bench
    PRIVATE
        cpp_exchange
)

target_include_directories(latency_bench PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}")

# recorded in every result file, so numbers always say how they were built
string(TOUPPER "${CMAKE_BUILD_TYPE}" VORTEX_BUILD_TYPE_UPPER)
target_compile_definitions(latency_bench PRIVATE
    VORTEX_CXX_FLAGS="${CMAKE_CXX_FLAGS} ${CMAKE_CXX_FLAGS_${VORTEX_BUILD_TYPE_UPPER}} -std=c++20 -fno-exceptions")

set_target_properties(
    latency_bench
    PROPERTIES
        RUNTIME_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/bench"
)

# ---- tests ----
enable_testing()
find_package(GTest 1.14 REQUIRED)
include(GoogleTest)

add_executable(
    engine_tests
    tests/golden_replay_test.cpp
    tests/spsc_queue_test.cpp
)

target_link_libraries(
    engine_tests
    PRIVATE
        cpp_exchange
        GTest::gtest_main
        Threads::Threads
)

set_target_properties(
    engine_tests
    PROPERTIES
        RUNTIME_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/tests"
)

target_include_directories(engine_tests PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}")
target_compile_definitions(engine_tests PRIVATE
    VORTEX_GOLDEN_DIR="${CMAKE_CURRENT_SOURCE_DIR}/tests/golden")

# one ctest entry per TEST(); discovered when ctest runs, not at build time
gtest_discover_tests(engine_tests DISCOVERY_MODE PRE_TEST)
