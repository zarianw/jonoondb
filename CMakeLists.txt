cmake_minimum_required (VERSION 2.8)

project (JonoonDB)

set (SRC_PATH ${PROJECT_SOURCE_DIR}/src/)
set (INCLUDE_PATH ${PROJECT_SOURCE_DIR}/include/)
set (THIRDPARTY_PATH ${PROJECT_SOURCE_DIR}/thirdparty)
set (TEST_PATH ${PROJECT_SOURCE_DIR}/tests)

# add the include directories
include_directories(${INCLUDE_PATH} ${INCLUDE_PATH}/jonoondb_api ${THIRDPARTY_PATH})

if(APPLE)
  set(CMAKE_CXX_FLAGS
    "${CMAKE_CXX_FLAGS} -std=c++14 -stdlib=libc++ -m64")
elseif(CMAKE_COMPILER_IS_GNUCXX OR "${CMAKE_CXX_COMPILER_ID}" MATCHES "Clang")
  set(CMAKE_CXX_FLAGS
    "${CMAKE_CXX_FLAGS} -std=c++14 -m64")
endif()

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
 ${THIRDPARTY_PATH}/flatbuffers-1.3.0/include/flatbuffers/reflection.h ${THIRDPARTY_PATH}/flatbuffers-1.3.0/src/reflection.cpp)
include_directories(${THIRDPARTY_PATH}/flatbuffers-1.3.0/include)
#add_subdirectory(${THIRDPARTY_PATH}/flatbuffers-1.3.0)

# boost
set(Boost_USE_STATIC_LIBS        ON) # only find static libs
set(Boost_USE_MULTITHREADED      ON)
set(Boost_USE_STATIC_RUNTIME    OFF)
find_package(Boost 1.60.0 EXACT REQUIRED COMPONENTS date_time chrono filesystem system thread program_options)
include_directories(${Boost_INCLUDE_DIR})

message("Using Boost_INCLUDE_DIR = ${Boost_INCLUDE_DIR}")
message("Using Boost_LIBRARY_DIRS = ${Boost_LIBRARY_DIRS}")

# LZ4
SET(BUILD_TOOLS OFF CACHE BOOL "Build LZ4 command line tools")
add_subdirectory(${THIRDPARTY_PATH}/lz4-r131/cmake_unofficial)
include_directories(${THIRDPARTY_PATH}/lz4-r131/lib)

# jonoondb_api
add_library(
 jonoondb_api 
 ${INCLUDE_PATH}/jonoondb_api/standard_deleters.h 
 ${INCLUDE_PATH}/jonoondb_api/string_utils.h
 ${INCLUDE_PATH}/jonoondb_api/serializer_utils.h 
 ${INCLUDE_PATH}/jonoondb_api/sqlite_utils.h
 ${INCLUDE_PATH}/jonoondb_api/document.h
 ${INCLUDE_PATH}/jonoondb_api/document_schema.h
 ${INCLUDE_PATH}/jonoondb_api/enums.h
 ${INCLUDE_PATH}/jonoondb_api/blob_metadata.h 
 ${INCLUDE_PATH}/jonoondb_api/concurrent_lru_cache.h
 ${INCLUDE_PATH}/jonoondb_api/memory_mapped_file.h
 ${INCLUDE_PATH}/jonoondb_api/file_info.h
 ${INCLUDE_PATH}/jonoondb_api/indexer.h
 ${INCLUDE_PATH}/jonoondb_api/ewah_compressed_bitmap_indexer_integer.h
 ${INCLUDE_PATH}/jonoondb_api/ewah_compressed_bitmap_indexer_string.h
 ${INCLUDE_PATH}/jonoondb_api/ewah_compressed_bitmap_indexer_double.h
 ${INCLUDE_PATH}/jonoondb_api/field.h
 ${INCLUDE_PATH}/jonoondb_api/jonoondb_exceptions.h 
 ${INCLUDE_PATH}/jonoondb_api/database.h
 ${INCLUDE_PATH}/jonoondb_api/concurrent_map.h
 ${INCLUDE_PATH}/jonoondb_api/index_info_fb_generated.h
 ${INCLUDE_PATH}/jonoondb_api/constraint.h
 ${INCLUDE_PATH}/jonoondb_api/file.h
 ${INCLUDE_PATH}/jonoondb_api/object_pool.h
 ${INCLUDE_PATH}/jonoondb_api/path_utils.h
 ${INCLUDE_PATH}/jonoondb_api/vector_integer_indexer.h
 ${INCLUDE_PATH}/jonoondb_api/vector_double_indexer.h
 ${INCLUDE_PATH}/jonoondb_api/vector_string_indexer.h
 ${INCLUDE_PATH}/jonoondb_api/null_helpers.h
 ${INCLUDE_PATH}/jonoondb_api/proc_utils.h
 ${SRC_PATH}/jonoondb_api/jonoondb_vtable.cc
 ${SRC_PATH}/jonoondb_api/exception_utils.cc ${INCLUDE_PATH}/jonoondb_api/exception_utils.h
 ${SRC_PATH}/jonoondb_api/cdatabase.cc ${INCLUDE_PATH}/jonoondb_api/cdatabase.h
 ${SRC_PATH}/jonoondb_api/database_impl.cc ${INCLUDE_PATH}/jonoondb_api/database_impl.h
 ${SRC_PATH}/jonoondb_api/status_impl.cc ${INCLUDE_PATH}/jonoondb_api/status_impl.h
 ${SRC_PATH}/jonoondb_api/options_impl.cc ${INCLUDE_PATH}/jonoondb_api/options_impl.h
 ${SRC_PATH}/jonoondb_api/database_metadata_manager.cc ${INCLUDE_PATH}/jonoondb_api/database_metadata_manager.h
 ${SRC_PATH}/jonoondb_api/document_collection.cc ${INCLUDE_PATH}/jonoondb_api/document_collection.h
 ${SRC_PATH}/jonoondb_api/index_info_impl.cc ${INCLUDE_PATH}/jonoondb_api/index_info_impl.h 
 ${SRC_PATH}/jonoondb_api/buffer_impl.cc ${INCLUDE_PATH}/jonoondb_api/buffer_impl.h
 ${SRC_PATH}/jonoondb_api/index_manager.cc ${INCLUDE_PATH}/jonoondb_api/index_manager.h
 ${SRC_PATH}/jonoondb_api/document_factory.cc ${INCLUDE_PATH}/jonoondb_api/document_factory.h
 ${SRC_PATH}/jonoondb_api/indexer_factory.cc ${INCLUDE_PATH}/jonoondb_api/indexer_factory.h
 ${SRC_PATH}/jonoondb_api/document_schema_factory.cc ${INCLUDE_PATH}/jonoondb_api/document_schema_factory.h
 ${SRC_PATH}/jonoondb_api/filename_manager.cc ${INCLUDE_PATH}/jonoondb_api/filename_manager.h  
 ${SRC_PATH}/jonoondb_api/mama_jennies_bitmap.cc ${INCLUDE_PATH}/jonoondb_api/mama_jennies_bitmap.h 
 ${SRC_PATH}/jonoondb_api/flatbuffers_document_schema.cc ${INCLUDE_PATH}/jonoondb_api/flatbuffers_document_schema.h
 ${SRC_PATH}/jonoondb_api/flatbuffers_document.cc ${INCLUDE_PATH}/jonoondb_api/flatbuffers_document.h
 ${SRC_PATH}/jonoondb_api/document_id_generator.cc ${INCLUDE_PATH}/jonoondb_api/document_id_generator.h 
 ${SRC_PATH}/jonoondb_api/query_processor.cc ${INCLUDE_PATH}/jonoondb_api/query_processor.h
 ${SRC_PATH}/jonoondb_api/flatbuffers_field.cc ${INCLUDE_PATH}/jonoondb_api/flatbuffers_field.h
 ${SRC_PATH}/jonoondb_api/document_collection_dictionary.cc ${INCLUDE_PATH}/jonoondb_api/document_collection_dictionary.h
 ${SRC_PATH}/jonoondb_api/guard_funcs.cc ${INCLUDE_PATH}/jonoondb_api/guard_funcs.h
 ${SRC_PATH}/jonoondb_api/resultset_impl.cc ${INCLUDE_PATH}/jonoondb_api/resultset_impl.h
 ${SRC_PATH}/jonoondb_api/index_stat.cc ${INCLUDE_PATH}/jonoondb_api/index_stat.h
 ${SRC_PATH}/jonoondb_api/blob_manager.cc ${INCLUDE_PATH}/jonoondb_api/blob_manager.h
 ${SRC_PATH}/jonoondb_api/id_seq.cc ${INCLUDE_PATH}/jonoondb_api/id_seq.h) 
 
target_link_libraries(jonoondb_api sqlite flatbuffers liblz4 ${Boost_LIBRARIES})

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

# gtest
# configure a header file to pass some of the CMake settings
# to the source code
set (TEST_FOLDER_PATH ${PROJECT_BINARY_DIR}/unittests)
set (RESOURCES_FOLDER_PATH ${PROJECT_SOURCE_DIR}/resources)
configure_file (
  "${INCLUDE_PATH}/test/test_config.h.in"
  "${INCLUDE_PATH}/test/test_config_generated.h")

add_subdirectory(${THIRDPARTY_PATH}/googletest-release-1.7.0)
enable_testing(true)
include_directories(${gtest_SOURCE_DIR}/include)

# jonoondb_test
add_executable(
 jonoondb_api_test
 ${TEST_PATH}/jonoondb_api/test_utils.h
 ${TEST_PATH}/jonoondb_api/main.cc
 ${TEST_PATH}/jonoondb_api/status_impl_tests.cc
 ${TEST_PATH}/jonoondb_api/database_metadata_manager_tests.cc
 ${TEST_PATH}/jonoondb_api/buffer_tests.cc
 ${TEST_PATH}/jonoondb_api/concurrent_lru_cache_tests.cc
 ${TEST_PATH}/jonoondb_api/memory_mapped_file_tests.cc
 ${TEST_PATH}/jonoondb_api/database_tests.cc
 ${TEST_PATH}/jonoondb_api/document_tests.cc
 ${TEST_PATH}/jonoondb_api/jonoondb_exception_tests.cc
 ${TEST_PATH}/jonoondb_api/filename_manager_tests.cc
 ${TEST_PATH}/jonoondb_api/flatbuffers_document_tests.cc
 ${TEST_PATH}/jonoondb_api/blob_manager_tests.cc
 ${TEST_PATH}/jonoondb_api/object_pool_tests.cc
 ${TEST_PATH}/jonoondb_api/options_tests.cc
 ${TEST_PATH}/jonoondb_api/index_info_tests.cc
 ${TEST_PATH}/jonoondb_api/database_sql_tests.cc
 ${TEST_PATH}/jonoondb_api/proc_utils_tests.cc
 ${TEST_PATH}/jonoondb_utils/varint_tests.cc)
target_link_libraries(jonoondb_api_test gtest gtest_main jonoondb_api ${Boost_LIBRARIES} rt)

include(CTest)
add_test(all_jonoondb_api_tests jonoondb_api_test)

#installation
SET(CMAKE_INSTALL_PREFIX ${PROJECT_BINARY_DIR})
install(TARGETS jonoondb_api DESTINATION install_packages)
install(TARGETS jonoondb_cli DESTINATION install_packages)
install(FILES ${INCLUDE_PATH}/jonoondb_api/database.h
 ${INCLUDE_PATH}/jonoondb_api/enums.h
 ${INCLUDE_PATH}/jonoondb_api/jonoondb_exceptions.h
 ${INCLUDE_PATH}/jonoondb_api/cdatabase.h
 DESTINATION install_packages)
