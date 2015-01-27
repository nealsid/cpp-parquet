// Copyright 2014 Mount Sinai School of Medicine

#include "./parquet_types.h"
#include <thrift/protocol/TCompactProtocol.h>
#include <glog/logging.h>
#include <boost/shared_ptr.hpp>
#include <string>
#include <vector>

#ifndef PARQUET_FILE_PARQUET_COLUMN_H_
#define PARQUET_FILE_PARQUET_COLUMN_H_

using boost::shared_ptr;
using parquet::ColumnChunk;
using parquet::ColumnMetaData;
using parquet::CompressionCodec;
using parquet::DataPageHeader;
using parquet::Encoding;
using parquet::FieldRepetitionType;
using parquet::Type;
using std::string;
using std::to_string;
using std::vector;

namespace parquet_file {
const int kDataBufferSize = 1024000;

// ParquetColumn represents a Parquet Column of data.  ParquetColumn
// can contain children, which is how an, for example, Apache Avro
// message could be represented.
class ParquetColumn {
 public:
  // Constructor for ParquetColumn.  name is a vector of column names
  // from the root of the schema to the current node.  Type is the
  // Parquet data type, repetition_type is the repetition type for the
  // column (repeated, required, etc), and encoding & compression are
  // as they are in Parquet.  max_{repetition, definition}_level
  // represents the max level of this column in the schema tree (it's
  // used for setting the repetition & definition levels)
  ParquetColumn(const vector<string>& column_name,
                parquet::Type::type data_type,
                uint16_t max_repetition_level,
                uint16_t max_definition_level,
                FieldRepetitionType::type repetition_type,
                Encoding::type encoding,
                CompressionCodec::type compression_codec);

  // Constructor for a container column.
  ParquetColumn(const vector<string>& column_name,
                FieldRepetitionType::type repetition_type);

  // Set/get the children of this column
  void SetChildren(const vector<ParquetColumn*>& children);
  void AddChild(ParquetColumn* child);
  const vector<ParquetColumn*>& Children() const;

  // Accessors for the reptition type, encoding, type, and name.
  FieldRepetitionType::type RepetitionType() const;
  Encoding::type Encoding() const;
  Type::type Type() const;
  CompressionCodec::type CompressionCodec() const;
  string Name() const;

  // A '.'-joined string of the path components (i.e. the names of
  // each containing column from the schema tree root to this leaf)
  string FullSchemaPath() const;

  // Method that returns the number of bytes for a given Parquet data type
  static uint8_t BytesForDataType(Type::type dataType);

  // Method that adds some data to this column.  Each datum in buf
  // is considered it's own record, if this field is repeated.
  void AddRecords(void* buf, uint16_t repetition_level, uint32_t n);
  // Adds repeated data to this column.  All data is considered part
  // of the same record.
  void AddRepeatedData(void *buf, uint16_t current_repetition_level,
                       uint32_t n);
  // Add a NULL to this column.
  void AddNulls(uint16_t current_repetition_level,
                uint16_t current_definition_level,
                uint32_t n);

  uint32_t NumRecords() const;
  uint32_t NumDatums() const;


  // Flush this column via the protocol provided.
  void Flush(int fd, apache::thrift::protocol::TCompactProtocol* protocol);

  // Generate a Parquet Thrift ColumnMetaData message for this column.
  ColumnMetaData ParquetColumnMetaData() const;
  // Pretty printing method.
  string ToString() const;

 private:
  void FlushLevels(int fd, const vector<uint8_t>& levels_array);

  // Helper method to encode a vector of 8-bit integers into an output
  // buffer.  Used for repetition & definition level encoding.
  void EncodeLevels(const vector<uint8_t>& level_vector,
                    vector<uint8_t>* output_vector,
                    uint16_t max_level);

  // Following two methods call EncodeLevels with the right parameters
  // for encoding those specific level vectors (repetition or
  // definition)
  void EncodeRepetitionLevels(vector<uint8_t>* encoded_repetition_levels);
  void EncodeDefinitionLevels(vector<uint8_t>* encoded_definition_levels);

  // The name of the column as a vector of strings from the root to
  // the current node.
  const vector<string> column_name_;
  // Parquet type indicating whether the field is required, or
  // repeated, etc.
  FieldRepetitionType::type repetition_type_;
  // Parquet enum value indicating encoding of this column.
  Encoding::type encoding_;
  // Parquet datatype for this column.
  Type::type data_type_;
  // Compression codec for this column
  CompressionCodec::type compression_codec_;
  // A list of columns that are children of this one.
  vector<ParquetColumn*> children_;

  // Bookkeeping
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
  // The number of bytes each instance of the datatype stored in this
  // column takes.
  uint8_t bytes_per_datum_;
  // Data buffer
  unsigned char data_buffer_[kDataBufferSize];
  // Current data pointer;
  unsigned char* data_ptr_;
  // Repetition level array. Run-length encoded before being written.
  vector<uint8_t> repetition_levels_;
  // Integer representing max repetition level in the schema tree.
  uint16_t max_repetition_level_;
  // Integer representing max definition level.
  uint16_t max_definition_level_;
  // Definition level array.  Also RLE before being written.
  vector<uint8_t> definition_levels_;
  // The offset into the file where column data is written.
  off_t column_write_offset_;
};

}  // namespace parquet_file


#endif  // PARQUET_FILE_PARQUET_COLUMN_H_
