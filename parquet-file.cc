// Copyright 2014 Mount Sinai School of Medicine.

#include "parquet-file.h"

#include <boost/shared_ptr.hpp>
#include <fcntl.h>
#include <glog/logging.h>
#include <parquet_types.h>
#include <string>
#include <thrift/protocol/TCompactProtocol.h>
#include <thrift/transport/TFDTransport.h>
#include <thrift/protocol/TJSONProtocol.h>

using apache::thrift::transport::TFDTransport;
using apache::thrift::protocol::TCompactProtocol;
using apache::thrift::protocol::TJSONProtocol;

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

const char* kParquetMagicBytes = "PAR1";

namespace parquet_file {

ParquetFile::ParquetFile(string file_base, int num_files) {
  // TODO: remove this restrction
  assert(num_files == 1);

  ok_ = false;

  fd_ = open(file_base.c_str(), O_RDWR | O_CREAT | O_EXCL, 0700);
  LOG_IF(FATAL, fd_ == -1) << "Could not create file " << file_base.c_str() << ": "
			     << strerror(errno);
  // Write magic header
  write(fd_, kParquetMagicBytes, strlen(kParquetMagicBytes));

  // Construct transports (provided by Thrift) for writing to the
  // file.
  file_transport_.reset(new TFDTransport(fd_));
  protocol_.reset(new TCompactProtocol(file_transport_));

  // Parquet-specific metadata for the file.
  file_meta_data_.__set_version(1);
  file_meta_data_.__set_created_by("Neal sid");

  ok_ = true;
  return;
}

void ParquetFile::DepthFirstSchemaTraversal(const ParquetColumn* root_column,
                                            ParquetColumnWalker* callback) {
  callback->ColumnCallback(root_column);
  const vector<ParquetColumn*>& children = root_column->Children();
  for (ParquetColumn* c : children) {
    DepthFirstSchemaTraversal(root_column, callback);
  }
}

void ParquetFile::Flush() {
  boost::shared_ptr<TFDTransport> stderr_transport_ptr(new TFDTransport(2));
  TJSONProtocol json_output(stderr_transport_ptr);

  LOG_IF(FATAL, file_columns_.size() == 0) <<
    "No columns to flush";
  off_t current_offset = lseek(fd_, 0, SEEK_CUR);
  // Make sure we know where we are in the file.  Also serves as a
  // somewhat weak guarantee that someone else hasn't written to the
  // file already.
  VLOG(2) << "Offset at beginning of flush: " << to_string(current_offset);
  assert(current_offset == strlen(kParquetMagicBytes));
  ParquetColumn* first_column = *file_columns_.begin();
  uint32_t num_rows = first_column->NumRows();
  LOG_IF(FATAL, num_rows == 0) 
    << "Number of rows in first column (name: " 
    << first_column->Name() << ") is 0";
  for (auto column : file_columns_) {
    LOG_IF(FATAL, column->NumRows() != num_rows)
      << "Columns must have the same number of rows.  "
      << "Differing column: " << column->Name()
      << ", Number of rows: " << column->NumRows()
      << ", expected number of rows: " << num_rows;
  }
  VLOG(2) << "Number of rows of data: " << num_rows;
  file_meta_data_.__set_num_rows(num_rows);

  RowGroup row_group;
  row_group.__set_num_rows(num_rows);
  row_group.__set_total_byte_size(0);
  vector<ColumnChunk> column_chunks;
  for (auto column : file_columns_) {
    VLOG(2) << "Writing column: " << column->Name();
    VLOG(2) << "\t" << column->ToString();
    column->Flush(fd_, protocol_.get());
    ColumnMetaData column_metadata = column->ParquetColumnMetaData();
    row_group.__set_total_byte_size(row_group.total_byte_size + 
				    column_metadata.total_uncompressed_size);
    VLOG(2) << "\tWrote " << to_string(column_metadata.total_uncompressed_size) 
	    << " bytes.";
    ColumnChunk column_chunk;
    column_chunk.__set_file_path(file_base_.c_str());
    column_chunk.__set_file_offset(column_metadata.data_page_offset);
    column_chunk.__set_meta_data(column_metadata);
    column_chunks.push_back(column_chunk);
  }
  VLOG(2) << "Total bytes for all columns: " << row_group.total_byte_size;
  row_group.__set_columns(column_chunks);
  file_meta_data_.__set_row_groups({row_group});
  uint32_t file_metadata_length = file_meta_data_.write(protocol_.get());
  VLOG(2) << "File metadata length: " << file_metadata_length;
  write(fd_, &file_metadata_length, sizeof(file_metadata_length));
  write(fd_, kParquetMagicBytes, strlen(kParquetMagicBytes));
  file_meta_data_.write(&json_output);
  VLOG(2) << "Done.";
}

void ParquetFile::SetSchema(const vector<ParquetColumn*>& schema) {
  SchemaElement root_column;
  // Name of the root column is irrelevant, as it's a dummy head node.
  root_column.__set_name("root");
  // For now, assume that we only have one level in the schema tree
  // (i.e. no nesting at the root)
  root_column.__set_num_children(schema.size());
  // Parquet's metadata needs the schema as a list, which results from
  // a depth-first traversal of the schema as a tree.
  vector<SchemaElement> parquet_schema_vector;
  parquet_schema_vector.push_back(root_column);
  ParquetColumnWalker* walker = new ParquetColumnWalker(&parquet_schema_vector);
  for (auto column : schema) {
    VLOG(2) << column->ToString();
    DepthFirstSchemaTraversal(column, walker);
  }
  LOG_IF(WARNING, file_columns_.size() > 0) 
    << "Internal file columns being reset";
  file_columns_.clear();
  file_columns_.assign(schema.begin(), schema.end());
  file_meta_data_.__set_schema(parquet_schema_vector);
}

ParquetColumnWalker::ParquetColumnWalker(vector<SchemaElement>* dfsVector) 
    : dfsVector_(dfsVector) {
}

void ParquetColumnWalker::ColumnCallback(const ParquetColumn* column) {
  // TODO: remove this restriction
  assert(column->Children().size() == 0);
  SchemaElement schemaElement;
  schemaElement.__set_name(column->Name());
  schemaElement.__set_repetition_type(column->RepetitionType());
  // Parquet requires that we don't set the number of children if
  // the schema element is for a data column.
  if (column->Children().size() > 0) {
    schemaElement.__set_num_children(column->Children().size());
  } else {
    schemaElement.__set_type(column->Type());
  }
  dfsVector_->push_back(schemaElement);
}


}  // namespace parquet_file
