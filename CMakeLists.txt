#
# Build CMakeFile for cpp-parquet

CMAKE_MINIMUM_REQUIRED(VERSION 2.8)
PROJECT(cpp-parquet-proj)

# Set to use C++11

ADD_DEFINITIONS(-std=c++11)

# Source files for main executable.
ADD_EXECUTABLE(cpp-parquet parquet-file-driver.cc parquet-file.cc parquet-column.cc)
SET(CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH} "${CMAKE_SOURCE_DIR}/cmake/Modules/")

# External projects for parquet-format and thrift
INCLUDE(ExternalProject)
INCLUDE(ParquetFormatCMakeLists)
INCLUDE(GoogleGlogCMakeLists)

FIND_PACKAGE(Thrift REQUIRED)
INCLUDE_DIRECTORIES(${THRIFT_INCLUDE_DIR})
SET(LIBS ${LIBS} ${THRIFT_LIBS})
ADD_LIBRARY(thriftstatic STATIC IMPORTED)
SET_TARGET_PROPERTIES(thriftstatic PROPERTIES IMPORTED_LOCATION ${THRIFT_STATIC_LIB})

ExternalProject_Get_Property(google-glog install_dir)
include_directories (${install_dir}/include)

ADD_DEPENDENCIES(cpp-parquet parquet-thrift glog thriftstatic)
TARGET_LINK_LIBRARIES(cpp-parquet parquet-thrift glog thriftstatic)

