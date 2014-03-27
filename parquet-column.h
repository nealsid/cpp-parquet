// Copyright 2014 Mount Sinai School of Medicine

#include "./parquet_types.h"
#include <thrift/protocol/TCompactProtocol.h>
#include <glog/logging.h>
#include <boost/shared_ptr.hpp>
#include <string>
#include <vector>

#ifndef PARQUET_COLUMN_H__
#define PARQUET_COLUMN_H__

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
    // Takes a name and the reptition type (the enum is defined by
    // Parquet)
    ParquetColumn(const string& name, Type::type data_type,
                  FieldRepetitionType::type repetition_type,
                  Encoding::type encoding,
                  CompressionCodec::type compression_codec);

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

    // Method that returns the number of bytes for a given Parquet data type
    static uint8_t BytesForDataType(Type::type dataType);

    // Method that adds some data to this column.  Each datum in buf
    // is considered it's own record, if this field is repeated.
    void AddRows(void* buf, uint32_t n);
    // Adds repeated data to this column.  All data is considered part
    // of the same record.
    void AddRepeatedData(void *buf, uint32_t n);
    uint32_t NumRows() const;
    void AddNull();

    // Flush this column via the protocol provided.
    // Having Flush() be const is a bit strange, but, technically, it
    // doesn't modify the class objects.
    void Flush(int fd, apache::thrift::protocol::TCompactProtocol* protocol);

    // Generate a Parquet Thrift ColumnMetaData message for this column.
    ColumnMetaData ParquetColumnMetaData() const;
    // Pretty printing method.
    string ToString() const;

   private:
    // Run-length encode a vector of numbers
    void RLE(const vector<uint8_t>& numbers, vector<uint32_t>* output);

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

    // Bookkeeping - how many rows are in this column?  This includes
    // NULLs and counts repeated fields as 1 row.
    uint32_t num_rows_;
    // The number of bytes each instance of the datatype stored in this
    // column takes.
    uint8_t bytes_per_datum_;
    // Data buffer
    unsigned char data_buffer_[kDataBufferSize];
    // Current data pointer;
    unsigned char* data_ptr_;
    // Repetition level array. Run-length encoded before being written.
    vector<uint8_t> repetition_levels_;
    // Integer representing current repetition level.  Keeping it as
    // uint16_t means that we can only support schemas that nest up to
    // 65536 repeated fields.  "64k nested fields ought to be enough for
    // anybody."
    uint16_t max_repetition_level_;
    // Definition level array.  Also RLE before being written.
    vector<uint8_t> definition_levels_;
    // Integer representing current definition level.
    uint16_t max_definition_level_;
    // The offset into the file where column data is written.
    off_t column_write_offset_;
  };

}  // namespace parquet_file


#endif  // PARQUET_COLUMN_H__
