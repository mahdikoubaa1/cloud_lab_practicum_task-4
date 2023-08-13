# Taken from: https://forgemia.inra.fr/pappso/masschroq/-/blob/masschroq_2.2.10-1/modules/FindRocksDB.cmake
#
# Try to find RocksDB headers and library.
#
# Usage of this module as follows:
#
#     find_package(RocksDB)
#
# Variables used by this module, they can change the default behaviour and need
# to be set before calling find_package:
#
#  ROCKSDB_ROOT_DIR          Set this variable to the root installation of
#                            RocksDB if the module has problems finding the
#                            proper installation path.
#
# Variables defined by this module:
#
#  ROCKSDB_FOUND               System has RocksDB library/headers.
#  ROCKSDB_LIBRARY             The RocksDB library.
#  ROCKSDB_INCLUDE_DIR         The location of RocksDB headers.

find_path(ROCKSDB_ROOT_DIR
        NAMES include/rocksdb/db.h)

find_library(ROCKSDB_LIBRARY
        NAMES rocksdb
        HINTS ${ROCKSDB_ROOT_DIR}/lib)

find_path(ROCKSDB_INCLUDE_DIR
        NAMES rocksdb/db.h
        HINTS ${ROCKSDB_ROOT_DIR}/include)

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(RocksDB DEFAULT_MSG
        ROCKSDB_LIBRARY
        ROCKSDB_INCLUDE_DIR)

mark_as_advanced(
        ROCKSDB_ROOT_DIR
        ROCKSDB_LIBRARY
        ROCKSDB_INCLUDE_DIR)