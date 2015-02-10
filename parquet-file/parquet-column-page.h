#include "./parquet_types.h"
#include <thrift/protocol/TCompactProtocol.h>
#include <glog/logging.h>
#include <string>
#include <vector>

#ifndef PARQUET_FILE_COLUMN_PAGE_H_
#define PARQUET_FILE_COLUMN_PAGE_H_

namespace parquet_file {

class ParquetColumnPage {
 public:
 private:
  // The number of bytes each instance of the datatype stored in this
  // column takes.
  const uint8_t bytes_per_datum_;
  // Data buffer for fixed-width data.
  uint8_t data_buffer_[kDataBufferSize];
  // Current data pointer;
  unsigned char* data_ptr_;

  // Repetition level array. Run-length encoded before being written.
  vector<uint8_t> repetition_levels_;
  // Definition level array.  Also RLE before being written.
  vector<uint8_t> definition_levels_;
  // How many did the page header + R&D levels + data take up?
  uint32_t uncompressed_bytes_;
  // how many records are in this column?  This includes
  // NULLs.  Repeated fields are counted as 1 record.
  uint32_t num_records_;
  // How many pieces of data are in this column.  For this field, repeated
  // data is not counted as one record.  So if you had an array field, and
  // an individual record contained [1,2,3,4,5],  num_datums_ would 5, and
  // num_records_ would be 1.
  uint32_t num_datums_;
};

}  // namespace parquet_file
#endif  // PARQUET_FILE_COLUMN_PAGE_H_
