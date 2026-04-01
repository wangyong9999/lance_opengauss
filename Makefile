# Makefile for lance_fdw (PGXS-compatible)
#
# Usage:
#   1. cargo build --release
#   2. make PG_CONFIG=/path/to/pg_config
#   3. make install PG_CONFIG=/path/to/pg_config

MODULE_big = lance_fdw
OBJS = src/lance_fdw.o src/arrow_to_pg.o

EXTENSION = lance_fdw
DATA = sql/lance_fdw--1.0.sql

RUST_TARGET_DIR ?= target
RUST_LIB = $(RUST_TARGET_DIR)/release/liblance_c.a

PG_CPPFLAGS = -I$(CURDIR)/src -std=c++14
SHLIB_LINK = $(RUST_LIB) -lpthread -ldl -lm -lrt -Wl,--exclude-libs,ALL

PG_CONFIG ?= pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)

# Ensure .cpp files compile as C++
%.o: %.cpp
	$(CXX) $(CXXFLAGS) $(PG_CPPFLAGS) $(CPPFLAGS) -fPIC -c -o $@ $<

$(OBJS): $(RUST_LIB)

$(RUST_LIB):
	cargo build --release
