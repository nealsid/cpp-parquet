// Copyright 2014 Mount Sinai School of Medicine

#include "parquet_types.h"
#include "thrift/protocol/TCompactProtocol.h"
#include "thrift/transport/TFDTransport.h"

#include <fcntl.h>

#include <boost/shared_ptr.hpp>
#include <string>
#include <vector>

#ifndef __PARQUET_FILE_H__
#define __PARQUET_FILE_H__

using apache::thrift::transport::TFDTransport;
using apache::thrift::protocol::TCompactProtocol;

using parquet::ColumnChunk;
using parquet::ColumnMetaData;
using parquet::CompressionCodec;
using parquet::DataPageHeader;
using parquet::Encoding;
using parquet::FieldRepetitionType;
using parquet::FileMetaData;
using parquet::PageHeader;
using parquet::PageType;
using parquet::RowGroup;
using parquet::SchemaElement;
using parquet::Type;

using std::string;
using std::vector;

const uint32_t kDataBytesPerPage = 81920000;

namespace parquet_file {
class ParquetColumn {
 public:
  ParquetColumn(string column_name, Type data_type, 
		FieldRepetitionType repetition_type);
  void AddValue(uint32_t data);
 private:
  vector<ParquetColumn*> children_;
  vector<ColumnChunk> data_chunks_;
  ColumnMetaData column_metadata_;
  DataPageHeader data_header_;

  /* ColumnChunk data for this column */
  string file_path_;
  uint64_t file_offset_;
  
  /* ColumnMetaData for this column */
  Type data_type_;
  FieldRepetitionType repetition_type_;
  string column_name_;
  
};

class ParquetFile {
 public:
  ParquetFile(string file_base, int num_files);
  ParquetColumn* AddField(string column_name, 
			  Type data_type, 
			  FieldRepetitionType repetition_type);
  void Flush();
  void Close();
 private:
  FileMetaData file_meta_data_;
  string file_base_;
  int num_files_;
  ParquetColumn* root_column;
};
}  // namespace parquet_file

#endif  // #ifndef __PARQUET_FILE_H__
