// Copyright 2014 Mount Sinai School of Medicine.
//
// This class repesents the base class for C++ representations of
// Parquet types.  These classes encapsulate properties such as bit
// width (32/64/96, etc), holding a value in a buffer of memory, and
// returning a value in little endian form suitable for writing to
// disk.

#include "./parquet_types.h"

#ifndef PARQUET_TYPES_PARQUET_TYPE_H_
#define PARQUET_TYPES_PARQUET_TYPE_H_

using parquet::Type;

namespace parquet_file {
const int kDataBufferSize = 1024000;

class ParquetDataBuffer {
 public:
  explicit ParquetDataBuffer(parquet::Type::type data_type) {
    this->data_type_ = data_type;
  }
  // Return the number of bytes that each piece of data of this type
  // takes up.
  static uint8_t BytesPerDatum(Type::type dataType) {
    // TODO support boolean (which is 1 bit)
    switch (dataType) {
      case Type::INT32:
      case Type::FLOAT:
        return 4;
      case Type::INT64:
      case Type::DOUBLE:
        return 8;
      case Type::INT96:
        return 12;
      case Type::BYTE_ARRAY:
      case Type::BOOLEAN:
      default:
        assert(0);
    }
  }

  // Adds a value to the buffer tracked by this object.
  void AddValue(void* buf) {
    AddNValues(buf, 1);
  }

  void AddNValues(void* buf, int n) {
    memcpy(data_buffer_, buf, n * BytesPerDatum(data_type_));
    num_values_ += n;
  }
  uint32_t NumValues() const { return num_values_; }
  void FetchValueLittleEndian(void *buf);

 private:
  // The parquet type of data for this data buffer.
  parquet::Type::type data_type_;
  // How many pieces of data are in this column.
  uint32_t num_values_;
  // Current data pointer;
  unsigned char* data_ptr_;
  // Data buffer
  unsigned char data_buffer_[kDataBufferSize];
};
}  // namespace parquet_file


#endif  // PARQUET_TYPES_PARQUET_TYPE_H_
