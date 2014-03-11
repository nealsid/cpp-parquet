// Copyright 2014 Mount Sinai School of Medicine

#include "parquet_types.h"
#include "thrift/protocol/TCompactProtocol.h"

#include <glog/logging.h>
#include <string>
#include <vector>

#ifndef __PARQUET_COLUMN_H__
#define __PARQUET_COLUMN_H__

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
// ParquetColumn represents a Parquet Column of data.  ParquetColumn
// can contain children, which is how an, for example, Apache Avro
// message could be represented.
class ParquetColumn {
public:
  // Takes a name and the reptition type (the enum is defined by
  // Parquet)
  ParquetColumn(const string& name, Type::type data_type, 
		FieldRepetitionType::type repetition_type,
		Encoding::type encoding,
		CompressionCodec::type compression_codec);

  // Set/get the children of this column
  void SetChildren(const vector<ParquetColumn*>& child);
  const vector<ParquetColumn*>& Children() const;

  // Accessors for the reptition type, encoding, type, and name.
  FieldRepetitionType::type RepetitionType() const;
  Encoding::type Encoding() const;
  Type::type Type() const;
  CompressionCodec::type CompressionCodec() const;
  string Name() const;

  // Method that returns the number of bytes for a given Parquet data type
  static uint8_t BytesForDataType(Type::type dataType);

  // Method that adds a row of data to this column
  void AddRows(void* buf, uint32_t n);
  uint32_t NumRows() const;

  // Flush this column via the protocol provided.
  void Flush(int fd, apache::thrift::protocol::TCompactProtocol* protocol);

  // Generate a Parquet Thrift ColumnMetaData message for this column.
  ColumnMetaData ParquetColumnMetaData() const;
  // Pretty printing method.
  string ToString() const;
 private:
  // The name of the column.
  string column_name_;
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

  // This represents the Parquet structures that will track this
  // column on-disk.  For now, we only support one chunk per column.
  // TODO: expand to multiple chunks
  ColumnChunk data_chunks_;
  ColumnMetaData column_metadata_;
  DataPageHeader data_header_;

  // Bookkeeping
  uint32_t num_rows_;
  // The number of bytes each instance of the datatype stored in this
  // column takes.
  uint8_t bytes_per_datum_;
  // Data buffer
  unsigned char data_buffer_[1024000];
  // Current data pointer;
  unsigned char* data_ptr_;
  // The offset into the file where column data is written.
  off_t column_write_offset_;
};

}  // namespace parquet_file

#endif  // #ifndef __PARQUET_COLUMN_H__
