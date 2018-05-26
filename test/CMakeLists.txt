cmake_minimum_required(VERSION 2.6)
project(luasqlgen-test)

find_package(GTest REQUIRED)

add_subdirectory(mariadbpp EXCLUDE_FROM_ALL)
add_executable(test main.cpp sqlite3/sqlite3.c)

target_include_directories(test PRIVATE mariadbpp/include sqlite3)
target_link_libraries(test mariadbclientpp dl gtest gtest_main)
