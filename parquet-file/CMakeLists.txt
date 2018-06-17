#
# Build CMakeFile for cpp-parquet

ADD_LIBRARY(libcppparquet parquet-file.cc parquet-column.cc)

ADD_CUSTOM_COMMAND(
   OUTPUT ${SOURCE_DIR}/src/gtest-all.cc
   COMMAND "touch ${SOURCE_DIR}/src/gtest-all.cc"
)

ExternalProject_Get_Property(googletest SOURCE_DIR)
INCLUDE_DIRECTORIES(${SOURCE_DIR}/googletest ${SOURCE_DIR}/googletest/include)

SET_SOURCE_FILES_PROPERTIES(${SOURCE_DIR}/googletest/src/gtest-all.cc PROPERTIES GENERATED 1)

ADD_EXECUTABLE(parquet-file-test parquet-file-test.cc ${SOURCE_DIR}/googletest/src/gtest-all.cc)
# Not sure why I had to do this instead of adding it to target_link_libraries below.
ADD_DEPENDENCIES(parquet-file-test googletest)
set_property(TARGET parquet-file-test PROPERTY DEPENDS googletest)

# Technically, this isn't quite correct. We assume that if the system
# has TR1/tuple, Google Test should use the system one.  But Google Test
# should only use the system one if both a) the system has TR1/TUPLE and
# b) the project is already pulling it in.  IF b isn't true, Google Test
# can still use it's own Tuple.  But this is good enough.
CHECK_INCLUDE_FILE_CXX("tr1/tuple" SYSTEM_HAS_TR1_TUPLE)
IF(SYSTEM_HAS_TR1_TUPLE)
	set_property(TARGET parquet-file-test PROPERTY COMPILE_FLAGS
	"-DGTEST_USE_OWN_TR1_TUPLE=0")
ENDIF(SYSTEM_HAS_TR1_TUPLE)

TARGET_LINK_LIBRARIES(parquet-file-test libcppparquet pthread glog thrift parquet-thrift)
