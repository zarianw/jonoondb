cmake_minimum_required (VERSION 2.8)

project (JonoonDB)

set (SRC_PATH ${PROJECT_SOURCE_DIR}/src/)
set (INCLUDE_PATH ${PROJECT_SOURCE_DIR}/include/)
set (INCLUDE_INTERNAL_PATH ${PROJECT_SOURCE_DIR}/include/internal)
set (TEST_PATH ${PROJECT_SOURCE_DIR}/test/)
set (THIRDPARTY_PATH ${PROJECT_SOURCE_DIR}/thirdparty)

# The version number.
# set (JonoonDB_VERSION_MAJOR 1)
# set (JonoonDB_VERSION_MINOR 0)
# configure a header file to pass some of the CMake settings
# to the source code
# configure_file (
#  "${PROJECT_SOURCE_DIR}/../include/jonoondb_config.h.in"
#  "${PROJECT_SOURCE_DIR}/../include/jonoondb_config.h"
#  )

# set (CMAKE_INSTALL_PREFIX "C:/Program Files/JonoonDB")
# add the install targets
# install (TARGETS jonoondb_api DESTINATION bin)

set (EXECUTABLE_OUTPUT_PATH "${PROJECT_SOURCE_DIR}/bin")
set (LIBRARY_OUTPUT_PATH "${PROJECT_SOURCE_DIR}/bin")

if(CMAKE_COMPILER_IS_GNUCXX)
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++11")
endif()

if(APPLE)
  set(CMAKE_CXX_FLAGS
    "${CMAKE_CXX_FLAGS} -std=c++11 -stdlib=libc++ -m64")
elseif(CMAKE_COMPILER_IS_GNUCXX OR "${CMAKE_CXX_COMPILER_ID}" MATCHES "Clang")
  set(CMAKE_CXX_FLAGS
    "${CMAKE_CXX_FLAGS} -std=c++11 -m64")
endif()


# add the include directories
include_directories(${INCLUDE_PATH} ${INCLUDE_INTERNAL_PATH} ${THIRDPARTY_PATH})

# sqlite
add_library(
 sqlite
 ${THIRDPARTY_PATH}/sqlite-amalgamation-3080803/sqlite3ext.h
 ${THIRDPARTY_PATH}/sqlite-amalgamation-3080803/sqlite3.c ${THIRDPARTY_PATH}/sqlite-amalgamation-3080803/sqlite3.h)
if(CMAKE_COMPILER_IS_GNUCXX)
  target_link_libraries(sqlite pthread dl)
endif()
include_directories(${THIRDPARTY_PATH}/sqlite-amalgamation-3080803)

# flatbuffers
add_library(
 flatbuffers
 ${THIRDPARTY_PATH}/flatbuffers/include/flatbuffers/flatbuffers.h
 ${THIRDPARTY_PATH}/flatbuffers/include/flatbuffers/hash.h
 ${THIRDPARTY_PATH}/flatbuffers/include/flatbuffers/idl.h
 ${THIRDPARTY_PATH}/flatbuffers/include/flatbuffers/util.h
 ${THIRDPARTY_PATH}/flatbuffers/src/idl_parser.cpp)
include_directories(${THIRDPARTY_PATH}/flatbuffers/include)

# boost
set(Boost_USE_STATIC_LIBS        ON) # only find static libs
set(Boost_USE_MULTITHREADED      ON)
set(Boost_USE_STATIC_RUNTIME    OFF)
find_package(Boost 1.56.0 REQUIRED COMPONENTS filesystem system thread date_time chrono)
include_directories(${Boost_INCLUDE_DIR})

message("Using Boost_INCLUDE_DIR = ${Boost_INCLUDE_DIR}")
message("Using Boost_LIBRARY_DIRS = ${Boost_LIBRARY_DIRS}")

# jonoondb_api
add_library(
 jonoondb_api 
 ${INCLUDE_PATH}/standard_deleters.h 
 ${INCLUDE_INTERNAL_PATH}/string_utils.h
 ${INCLUDE_INTERNAL_PATH}/serializer_utils.h
 ${INCLUDE_INTERNAL_PATH}/exception_utils.h
 ${INCLUDE_INTERNAL_PATH}/sqlite_utils.h
 ${INCLUDE_PATH}/document.h
 ${INCLUDE_PATH}/document_schema.h
 ${INCLUDE_PATH}/enums.h
 ${INCLUDE_INTERNAL_PATH}/blob_metadata.h 
 ${INCLUDE_INTERNAL_PATH}/concurrent_lru_cache.h
 ${INCLUDE_INTERNAL_PATH}/memory_mapped_file.h
 ${INCLUDE_INTERNAL_PATH}/file_info.h
 ${INCLUDE_PATH}/indexer.h
 ${INCLUDE_PATH}/constants.h
 ${INCLUDE_INTERNAL_PATH}/ewah_compressed_bitmap_indexer.h 
 ${INCLUDE_PATH}/field.h
 ${INCLUDE_PATH}/jonoondb_exceptions.h
 ${INCLUDE_PATH}/cdatabase.h
 ${INCLUDE_PATH}/cenums.h
 ${INCLUDE_PATH}/ex_database.h
 ${SRC_PATH}/jonoondb_api/cdatabase.cc
 ${SRC_PATH}/jonoondb_api/jonoondb_vtable.cc
 ${SRC_PATH}/jonoondb_api/database.cc ${INCLUDE_PATH}/database.h
 ${SRC_PATH}/jonoondb_api/database_impl.cc ${INCLUDE_PATH}/database_impl.h
 ${SRC_PATH}/jonoondb_api/status.cc ${INCLUDE_PATH}/status.h
 ${SRC_PATH}/jonoondb_api/options.cc ${INCLUDE_PATH}/options.h
 ${SRC_PATH}/jonoondb_api/database_metadata_manager.cc ${INCLUDE_INTERNAL_PATH}/database_metadata_manager.h
 ${SRC_PATH}/jonoondb_api/document_collection.cc ${INCLUDE_INTERNAL_PATH}/document_collection.h
 ${SRC_PATH}/jonoondb_api/index_info.cc ${INCLUDE_PATH}/index_info.h
 ${INCLUDE_INTERNAL_PATH}/index_info_fb_generated.h
 ${SRC_PATH}/jonoondb_api/buffer.cc ${INCLUDE_PATH}/buffer.h
 ${SRC_PATH}/jonoondb_api/index_manager.cc ${INCLUDE_INTERNAL_PATH}/index_manager.h
 ${SRC_PATH}/jonoondb_api/document_factory.cc ${INCLUDE_INTERNAL_PATH}/document_factory.h
 ${SRC_PATH}/jonoondb_api/indexer_factory.cc ${INCLUDE_INTERNAL_PATH}/indexer_factory.h
 ${SRC_PATH}/jonoondb_api/document_schema_factory.cc ${INCLUDE_INTERNAL_PATH}/document_schema_factory.h
 
 ${SRC_PATH}/jonoondb_api/mama_jennies_bitmap.cc ${INCLUDE_INTERNAL_PATH}/mama_jennies_bitmap.h
 ${SRC_PATH}/jonoondb_api/flatbuffers_document_schema.cc ${INCLUDE_INTERNAL_PATH}/flatbuffers_document_schema.h
 ${SRC_PATH}/jonoondb_api/flatbuffers_document.cc ${INCLUDE_INTERNAL_PATH}/flatbuffers_document.h
 ${SRC_PATH}/jonoondb_api/field_name_parser.cc ${INCLUDE_INTERNAL_PATH}/field_name_parser.h
 ${SRC_PATH}/jonoondb_api/document_id_generator.cc ${INCLUDE_INTERNAL_PATH}/document_id_generator.h 
 ${SRC_PATH}/jonoondb_api/query_processor.cc ${INCLUDE_INTERNAL_PATH}/query_processor.h
 ${SRC_PATH}/jonoondb_api/flatbuffers_field.cc ${INCLUDE_INTERNAL_PATH}/flatbuffers_field.h
 ${SRC_PATH}/jonoondb_api/document_collection_dictionary.cc ${INCLUDE_INTERNAL_PATH}/document_collection_dictionary.h
 ${SRC_PATH}/jonoondb_api/guard_funcs.cc ${INCLUDE_INTERNAL_PATH}/guard_funcs.h
 ${SRC_PATH}/jonoondb_api/resultset.cc ${INCLUDE_PATH}/resultset.h
 ${SRC_PATH}/jonoondb_api/index_stat.cc ${INCLUDE_INTERNAL_PATH}/index_stat.h)
 
target_link_libraries(jonoondb_api sqlite flatbuffers ${Boost_LIBRARIES})

# gtest
SET(GTEST_PATH "/" CACHE PATH "Path where gtest is installed.")
message("Using GTEST_PATH = ${GTEST_PATH}")
add_subdirectory(${GTEST_PATH} ${GTEST_PATH}/bin)
enable_testing(true)
include_directories(${gtest_SOURCE_DIR}/include)

add_executable(
 jonoondb_test
 ${TEST_PATH}/test_utils.h
 ${TEST_PATH}/main.cc
 ${TEST_PATH}/status_tests.cc
 ${TEST_PATH}/database_metadata_manager_tests.cc
 ${TEST_PATH}/buffer_tests.cc
 ${TEST_PATH}/concurrent_lru_cache_tests.cc
 ${TEST_PATH}/memory_mapped_file_tests.cc
 ${TEST_PATH}/database_tests.cc
 ${TEST_PATH}/document_tests.cc
 ${TEST_PATH}/jonoondb_exception_tests.cc)
target_link_libraries(jonoondb_test gtest gtest_main jonoondb_api ${Boost_LIBRARIES})
#add_test(jonoondb_test jonoondb_test)
