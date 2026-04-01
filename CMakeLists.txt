cmake_minimum_required(VERSION 3.16)
project(lance_fdw CXX)

# lance_fdw: Lance Foreign Data Wrapper for openGauss
#
# Build: mkdir build && cd build && cmake .. && make
# Install: make install

# ============================================================
# Configuration
# ============================================================

find_program(PG_CONFIG pg_config REQUIRED)

execute_process(COMMAND ${PG_CONFIG} --includedir-server
    OUTPUT_VARIABLE PG_INCLUDEDIR_SERVER OUTPUT_STRIP_TRAILING_WHITESPACE)
execute_process(COMMAND ${PG_CONFIG} --pkglibdir
    OUTPUT_VARIABLE PG_PKGLIBDIR OUTPUT_STRIP_TRAILING_WHITESPACE)
execute_process(COMMAND ${PG_CONFIG} --sharedir
    OUTPUT_VARIABLE PG_SHAREDIR OUTPUT_STRIP_TRAILING_WHITESPACE)

set(RUST_TARGET_DIR "${CMAKE_CURRENT_BINARY_DIR}/rust-target")
set(RUST_LIB "${RUST_TARGET_DIR}/release/liblance_c.a")

# ============================================================
# Rust staticlib
# ============================================================

file(GLOB_RECURSE RUST_SOURCES
    ${CMAKE_CURRENT_SOURCE_DIR}/rust/*.rs
    ${CMAKE_CURRENT_SOURCE_DIR}/Cargo.toml)

add_custom_command(
    OUTPUT ${RUST_LIB}
    COMMAND ${CMAKE_COMMAND} -E env
        CARGO_TARGET_DIR=${RUST_TARGET_DIR}
        cargo build --release
    WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
    COMMENT "Building liblance_c.a"
    DEPENDS ${RUST_SOURCES}
)

add_custom_target(lance_c_lib DEPENDS ${RUST_LIB})

# ============================================================
# C++ shared library
# ============================================================

add_library(lance_fdw SHARED
    src/lance_fdw.cpp
    src/arrow_to_pg.cpp
)

set_target_properties(lance_fdw PROPERTIES
    PREFIX ""
    OUTPUT_NAME "lance_fdw"
    CXX_STANDARD 14
)

target_include_directories(lance_fdw PRIVATE
    ${PG_INCLUDEDIR_SERVER}
    ${CMAKE_CURRENT_SOURCE_DIR}/src
)

add_dependencies(lance_fdw lance_c_lib)
target_link_libraries(lance_fdw PRIVATE
    ${RUST_LIB} pthread dl m rt
)
target_link_options(lance_fdw PRIVATE -Wl,--exclude-libs,ALL)

# ============================================================
# Install
# ============================================================

install(TARGETS lance_fdw LIBRARY DESTINATION ${PG_PKGLIBDIR})
install(FILES lance_fdw.control sql/lance_fdw--1.0.sql
    DESTINATION ${PG_SHAREDIR}/extension)
