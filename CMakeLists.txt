cmake_minimum_required (VERSION 3.2)

project (JonoonDB)

set (SRC_PATH ${PROJECT_SOURCE_DIR}/src/)
set (INCLUDE_PATH ${PROJECT_SOURCE_DIR}/include/)
set (JONOONDB_API_INCLUDE_PATH ${PROJECT_SOURCE_DIR}/include/jonoondb_api)
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

if(APPLE)
  set(CMAKE_CXX_FLAGS
    "${CMAKE_CXX_FLAGS} -std=c++14 -stdlib=libc++ -m64")
elseif(CMAKE_COMPILER_IS_GNUCXX OR "${CMAKE_CXX_COMPILER_ID}" MATCHES "Clang")
  set(CMAKE_CXX_FLAGS
    "${CMAKE_CXX_FLAGS} -std=c++14 -m64")
endif()


# add the include directories
include_directories(${INCLUDE_PATH} ${JONOONDB_API_INCLUDE_PATH} ${THIRDPARTY_PATH})

#gsl
include_directories(${THIRDPARTY_PATH}/gsl/include)

# sqlite
add_library(
 sqlite
 ${THIRDPARTY_PATH}/sqlite-amalgamation-3110100-vec/sqlite3ext.h
 ${THIRDPARTY_PATH}/sqlite-amalgamation-3110100-vec/sqlite3.c ${THIRDPARTY_PATH}/sqlite-amalgamation-3110100-vec/sqlite3.h)
if(CMAKE_COMPILER_IS_GNUCXX)
  target_link_libraries(sqlite pthread dl)
endif()
include_directories(${THIRDPARTY_PATH}/sqlite-amalgamation-3110100-vec)

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
find_package(Boost 1.60.0 EXACT REQUIRED COMPONENTS filesystem system thread date_time chrono program_options)
include_directories(${Boost_INCLUDE_DIR})

message("Using Boost_INCLUDE_DIR = ${Boost_INCLUDE_DIR}")
message("Using Boost_LIBRARY_DIRS = ${Boost_LIBRARY_DIRS}")

# jonoondb_api
add_library(
 jonoondb_api 
 ${JONOONDB_API_INCLUDE_PATH}/standard_deleters.h 
 ${JONOONDB_API_INCLUDE_PATH}/string_utils.h
 ${JONOONDB_API_INCLUDE_PATH}/serializer_utils.h
 ${JONOONDB_API_INCLUDE_PATH}/exception_utils.h
 ${JONOONDB_API_INCLUDE_PATH}/sqlite_utils.h
 ${JONOONDB_API_INCLUDE_PATH}/document.h
 ${JONOONDB_API_INCLUDE_PATH}/document_schema.h
 ${JONOONDB_API_INCLUDE_PATH}/enums.h
 ${JONOONDB_API_INCLUDE_PATH}/blob_metadata.h 
 ${JONOONDB_API_INCLUDE_PATH}/concurrent_lru_cache.h
 ${JONOONDB_API_INCLUDE_PATH}/memory_mapped_file.h
 ${JONOONDB_API_INCLUDE_PATH}/file_info.h
 ${JONOONDB_API_INCLUDE_PATH}/indexer.h
 ${JONOONDB_API_INCLUDE_PATH}/constants.h
 ${JONOONDB_API_INCLUDE_PATH}/ewah_compressed_bitmap_indexer_integer.h
 ${JONOONDB_API_INCLUDE_PATH}/ewah_compressed_bitmap_indexer_string.h
 ${JONOONDB_API_INCLUDE_PATH}/ewah_compressed_bitmap_indexer_double.h
 ${JONOONDB_API_INCLUDE_PATH}/field.h
 ${JONOONDB_API_INCLUDE_PATH}/jonoondb_exceptions.h 
 ${JONOONDB_API_INCLUDE_PATH}/database.h
 ${JONOONDB_API_INCLUDE_PATH}/concurrent_map.h
 ${JONOONDB_API_INCLUDE_PATH}/index_info_fb_generated.h
 ${JONOONDB_API_INCLUDE_PATH}/constraint.h
 ${JONOONDB_API_INCLUDE_PATH}/file.h
 ${JONOONDB_API_INCLUDE_PATH}/object_pool.h
 ${JONOONDB_API_INCLUDE_PATH}/path_utils.h
 ${JONOONDB_API_INCLUDE_PATH}/vector_integer_indexer.h
 ${JONOONDB_API_INCLUDE_PATH}/vector_double_indexer.h
 ${JONOONDB_API_INCLUDE_PATH}/vector_string_indexer.h
 ${SRC_PATH}/jonoondb_api/jonoondb_vtable.cc
 ${SRC_PATH}/jonoondb_api/cdatabase.cc ${JONOONDB_API_INCLUDE_PATH}/cdatabase.h
 ${SRC_PATH}/jonoondb_api/database_impl.cc ${JONOONDB_API_INCLUDE_PATH}/database_impl.h
 ${SRC_PATH}/jonoondb_api/status_impl.cc ${JONOONDB_API_INCLUDE_PATH}/status_impl.h
 ${SRC_PATH}/jonoondb_api/options_impl.cc ${JONOONDB_API_INCLUDE_PATH}/options_impl.h
 ${SRC_PATH}/jonoondb_api/database_metadata_manager.cc ${JONOONDB_API_INCLUDE_PATH}/database_metadata_manager.h
 ${SRC_PATH}/jonoondb_api/document_collection.cc ${JONOONDB_API_INCLUDE_PATH}/document_collection.h
 ${SRC_PATH}/jonoondb_api/index_info_impl.cc ${JONOONDB_API_INCLUDE_PATH}/index_info_impl.h 
 ${SRC_PATH}/jonoondb_api/buffer_impl.cc ${JONOONDB_API_INCLUDE_PATH}/buffer_impl.h
 ${SRC_PATH}/jonoondb_api/index_manager.cc ${JONOONDB_API_INCLUDE_PATH}/index_manager.h
 ${SRC_PATH}/jonoondb_api/document_factory.cc ${JONOONDB_API_INCLUDE_PATH}/document_factory.h
 ${SRC_PATH}/jonoondb_api/indexer_factory.cc ${JONOONDB_API_INCLUDE_PATH}/indexer_factory.h
 ${SRC_PATH}/jonoondb_api/document_schema_factory.cc ${JONOONDB_API_INCLUDE_PATH}/document_schema_factory.h
 ${SRC_PATH}/jonoondb_api/filename_manager.cc ${JONOONDB_API_INCLUDE_PATH}/filename_manager.h  
 ${SRC_PATH}/jonoondb_api/mama_jennies_bitmap.cc ${JONOONDB_API_INCLUDE_PATH}/mama_jennies_bitmap.h
 ${SRC_PATH}/jonoondb_api/flatbuffers_document_schema.cc ${JONOONDB_API_INCLUDE_PATH}/flatbuffers_document_schema.h
 ${SRC_PATH}/jonoondb_api/flatbuffers_document.cc ${JONOONDB_API_INCLUDE_PATH}/flatbuffers_document.h
 ${SRC_PATH}/jonoondb_api/document_id_generator.cc ${JONOONDB_API_INCLUDE_PATH}/document_id_generator.h 
 ${SRC_PATH}/jonoondb_api/query_processor.cc ${JONOONDB_API_INCLUDE_PATH}/query_processor.h
 ${SRC_PATH}/jonoondb_api/flatbuffers_field.cc ${JONOONDB_API_INCLUDE_PATH}/flatbuffers_field.h
 ${SRC_PATH}/jonoondb_api/document_collection_dictionary.cc ${JONOONDB_API_INCLUDE_PATH}/document_collection_dictionary.h
 ${SRC_PATH}/jonoondb_api/guard_funcs.cc ${JONOONDB_API_INCLUDE_PATH}/guard_funcs.h
 ${SRC_PATH}/jonoondb_api/resultset_impl.cc ${JONOONDB_API_INCLUDE_PATH}/resultset_impl.h
 ${SRC_PATH}/jonoondb_api/index_stat.cc ${JONOONDB_API_INCLUDE_PATH}/index_stat.h
 ${SRC_PATH}/jonoondb_api/blob_manager.cc ${JONOONDB_API_INCLUDE_PATH}/blob_manager.h
 ${SRC_PATH}/jonoondb_api/id_seq.cc ${JONOONDB_API_INCLUDE_PATH}/id_seq.h) 
 
target_link_libraries(jonoondb_api sqlite flatbuffers ${Boost_LIBRARIES})

# gtest
SET(GTEST_PATH "/" CACHE PATH "Path where gtest is installed.")
message("Using GTEST_PATH = ${GTEST_PATH}")
add_subdirectory(${GTEST_PATH} ${GTEST_PATH}/bin)
enable_testing(true)
include_directories(${gtest_SOURCE_DIR}/include)

# jonoondb_test
add_executable(
 jonoondb_test
 ${TEST_PATH}/test_utils.h
 ${TEST_PATH}/main.cc
 ${TEST_PATH}/status_impl_tests.cc
 ${TEST_PATH}/database_metadata_manager_tests.cc
 ${TEST_PATH}/buffer_tests.cc
 ${TEST_PATH}/concurrent_lru_cache_tests.cc
 ${TEST_PATH}/memory_mapped_file_tests.cc
 ${TEST_PATH}/database_tests.cc
 ${TEST_PATH}/document_tests.cc
 ${TEST_PATH}/jonoondb_exception_tests.cc
 ${TEST_PATH}/filename_manager_tests.cc
 ${TEST_PATH}/flatbuffers_document_tests.cc
 ${TEST_PATH}/blob_manager_tests.cc
 ${TEST_PATH}/object_pool_tests.cc
 ${TEST_PATH}/options_tests.cc
 ${TEST_PATH}/index_info_tests.cc)
target_link_libraries(jonoondb_test gtest gtest_main jonoondb_api ${Boost_LIBRARIES})
#add_test(jonoondb_test jonoondb_test)


# Find a way to link against only the required boost libs
# ${Boost_LIBRARIES} points to all the libs in find_package
add_executable(
 jonoondb_cli
 ${SRC_PATH}/jonoondb_cli/main.cc)
target_link_libraries(jonoondb_cli jonoondb_api ${Boost_LIBRARIES})

# dbgen_converter
add_executable(
 dbgen_converter
 ${SRC_PATH}/dbgen_converter/main.cc)
target_link_libraries(dbgen_converter ${Boost_LIBRARIES})
