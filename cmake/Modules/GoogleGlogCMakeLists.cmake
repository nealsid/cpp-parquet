ExternalProject_Add(google-glog
   PREFIX ${CMAKE_BINARY_DIR}/third_party/build/google-glog
   GIT_REPOSITORY https://github.com/google/glog
   GIT_TAG v0.3.5
   CONFIGURE_COMMAND <SOURCE_DIR>/configure --prefix=<INSTALL_DIR>
   BUILD_COMMAND make
   INSTALL_COMMAND make install
   INSTALL_DIR ${CMAKE_BINARY_DIR}/third_party/google-glog
   # Update command has to be set to "", otherwise CMake will refetch
   # & rebuild every time.
   UPDATE_COMMAND ""
)

ExternalProject_Get_Property(google-glog install_dir)
INCLUDE_DIRECTORIES (${install_dir}/include)

# from www.cmake.org/pipermail/cmake/2011-June/045092.html For some
# reason, FIND_LIBRARY_USE_LIB64_PATHS is true on OS X, even though 64
# bit builds don't install into lib64 by default (maybe this is
# specific to Google Test)
get_property(LIB64 GLOBAL PROPERTY FIND_LIBRARY_USE_LIB64_PATHS)
if (${LIB64} STREQUAL "TRUE" AND NOT APPLE)
  set(LIBSUFFIX 64)
else()
  set(LIBSUFFIX "")
endif()

ADD_LIBRARY(glog STATIC IMPORTED)
SET_PROPERTY(TARGET glog PROPERTY IMPORTED_LOCATION ${CMAKE_BINARY_DIR}/third_party/google-glog/lib${LIBSUFFIX}/libglog.a)
ADD_DEPENDENCIES(glog google-glog)
