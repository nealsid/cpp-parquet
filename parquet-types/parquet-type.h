// Copyright 2014 Mount Sinai School of Medicine.
//
// This class repesents the base class for C++ representations of
// Parquet types.  These classes encapsulate properties such as bit
// width (32/64/96, etc), holding a value in a buffer of memory, and
// returning a value in little endian form suitable for writing to
// disk.

#include "./parquet_types.h"

class ParquetDataBuffer {
public:
  ParquetType() {

  }
  // Return the number of bytes that each piece of data of this type
  // takes up.
  uint8_t BytesPerDatum() const = 0;
  // Adds a value to the buffer tracked by this object.
  void AddValue(void* buf) = 0;
  void FetchValueLittleEndian();
  void AddValue(void* buf) {
    memcpy(buffer, buf, BytesPerDatum());
  }
private:

  void* buffer_;
  // Current data pointer;
  unsigned char* data_ptr_;
};
