#include "./parquet_types.h"
#include <parquet-file/parquet-column.h>
#include <thrift/protocol/TCompactProtocol.h>
#include <glog/logging.h>
#include <string>
#include <vector>

#ifndef PARQUET_FILE_COLUMN_PAGE_H_
#define PARQUET_FILE_COLUMN_PAGE_H_

namespace parquet_file {

class ParquetColumnPage {
 public:
  ParquetColumnPage(parquet::Type::type data_type,
                    uint16_t max_repetition_level,
                    uint16_t max_definition_level)
      : max_definition_level_(max_definition_level),
        max_repetition_level_(max_repetition_level),
        data_type_(data_type),
        data_ptr_(data_buffer_),
        bytes_per_datum_(ParquetColumn::BytesForDataType(data_type)) {
  };

  void AddRecords(void* buf, uint16_t repetition_level,
                  uint32_t n);
  void AddRepeatedData(void *buf,
                       uint16_t current_repetition_level,
                       uint32_t n);
  void AddNulls(uint16_t current_repetition_level,
                uint16_t current_definition_level,
                uint32_t n);

  uint32_t NumRecords() const {
    return num_records_;
  }

  uint32_t NumDatums() const {
    return num_datums_;
  }

 private:
  // The number of bytes each instance of the datatype stored in this
  // column takes.
  const uint8_t bytes_per_datum_;
  // Data buffer for fixed-width data.
  uint8_t data_buffer_[kDataBufferSize];
  // Current data pointer;
  uint8_t* data_ptr_;

  // Integer representing max repetition level in the schema tree.
  const uint8_t max_repetition_level_;
  // Integer representing max definition level.
  const uint8_t max_definition_level_;
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
  // Parquet datatype for this column.
  Type::type data_type_;
};

}  // namespace parquet_file
#endif  // PARQUET_FILE_COLUMN_PAGE_H_
