#
# Build CMakeFile for cpp-parquet

CMAKE_MINIMUM_REQUIRED(VERSION 2.8)
PROJECT(cpp-parquet-proj)

# Set to use C++11

ADD_DEFINITIONS(-std=c++11)
SET(CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH} "${CMAKE_SOURCE_DIR}/cmake/Modules/")

# External projects for parquet-format and thrift
INCLUDE(ExternalProject)
INCLUDE(ParquetFormatCMakeLists)
INCLUDE(GoogleGlogCMakeLists)
INCLUDE(GoogleTestCMakeLists)

FIND_PACKAGE(Thrift REQUIRED)
INCLUDE_DIRECTORIES(${CMAKE_SOURCE_DIR})
INCLUDE_DIRECTORIES(${THRIFT_INCLUDE_DIR})
SET(LIBS ${LIBS} ${THRIFT_LIBS})
ADD_LIBRARY(thriftstatic STATIC IMPORTED)
SET_TARGET_PROPERTIES(thriftstatic PROPERTIES IMPORTED_LOCATION ${THRIFT_STATIC_LIB})

ExternalProject_Get_Property(google-glog install_dir)
include_directories (${install_dir}/include)

ADD_SUBDIRECTORY(parquet-file)
ADD_SUBDIRECTORY(avro-schema)
ADD_SUBDIRECTORY(examples)
