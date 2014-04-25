ExternalProject_Add(snappy
   PREFIX ${CMAKE_BINARY_DIR}/third_party/build/snappy
   GIT_REPOSITORY https://github.com/google/snappy/
   GIT_TAG 1ff9be9b8fafc8528ca9e055646f5932aa5db9c4
   CONFIGURE_COMMAND <SOURCE_DIR>/configure --prefix=<INSTALL_DIR>
   BUILD_COMMAND make
   INSTALL_COMMAND make install
   INSTALL_DIR ${CMAKE_BINARY_DIR}/third_party/snappy
   # Update command has to be set to "", otherwise CMake will refetch
   # & rebuild every time.
   UPDATE_COMMAND ""
)

ExternalProject_Get_Property(snappy install_dir)
INCLUDE_DIRECTORIES (${install_dir}/include)

ADD_LIBRARY(libsnappy STATIC IMPORTED)
SET_PROPERTY(TARGET libsnappy PROPERTY IMPORTED_LOCATION ${CMAKE_BINARY_DIR}/third_party/snappy)
ADD_DEPENDENCIES(libsnappy snappy)
