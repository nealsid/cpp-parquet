// Copyright 2014 Mount Sinai School of Medicine.

#include "./parquet-file.h"

#include <boost/shared_ptr.hpp>
#include <fcntl.h>
#include <glog/logging.h>
#include <parquet_types.h>
#include <thrift/protocol/TCompactProtocol.h>
#include <thrift/transport/TFDTransport.h>
#include <thrift/protocol/TJSONProtocol.h>

#include <set>
#include <string>

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
using std::set;

const char* kParquetMagicBytes = "PAR1";

namespace parquet_file {

ParquetFile::ParquetFile(string file_base, int num_files) {
  // TODO: remove this restrction
  assert(num_files == 1);

  ok_ = false;

  fd_ = open(file_base.c_str(), O_RDWR | O_CREAT | O_EXCL, 0700);
  LOG_IF(FATAL, fd_ == -1) << "Could not create file " << file_base.c_str()
                           << ": " << strerror(errno);
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

void ParquetFile::DepthFirstSchemaTraversal(ParquetColumn* root_column,
                                            ParquetColumnWalker* callback) {
  callback->ColumnCallback(root_column);
  file_columns_.push_back(root_column);
  const vector<ParquetColumn*>& children = root_column->Children();
  for (ParquetColumn* c : children) {
    DepthFirstSchemaTraversal(c, callback);
  }
}

void ParquetFile::NumberOfRecords(set<uint64_t>* column_record_counts) const {
  CHECK_NOTNULL(column_record_counts);
  column_record_counts->clear();
  for (auto column = file_columns_.begin() + 1;
       column != file_columns_.end();
       ++column) {
    if ((*column)->Children().size() == 0) {
      column_record_counts->insert((*column)->NumRecords());
    }
  }
}

uint64_t ParquetFile::BytesForRecord(uint64_t record_index) const {
  uint64_t record_size = 0;
  for (auto column = file_columns_.begin() + 1;
       column != file_columns_.end();
       ++column) {
    if ((*column)->Children().size() == 0) {
      record_size += (*column)->recordSize(record_index);
    }
  }
  return record_size;
}

void ParquetFile::Flush() {
  LOG_IF(FATAL, file_columns_.size() == 0) <<
    "No columns to flush";
  off_t current_offset = lseek(fd_, 0, SEEK_CUR);
  // Make sure we know where we are in the file.  Also serves as a
  // somewhat weak guarantee that someone else hasn't written to the
  // file already.
  VLOG(2) << "Offset at beginning of flush: " << to_string(current_offset);
  assert(current_offset == strlen(kParquetMagicBytes));

  set<uint64_t> column_record_counts;
  NumberOfRecords(&column_record_counts);
  LOG_IF(FATAL, column_record_counts.size() > 1)
      << "All columns must have the same number of records: "
      << column_record_counts.size();

  uint64_t num_records = *(column_record_counts.begin());
  LOG_IF(WARNING,  num_records == 0)
    << "Number of records in first leaf-node column is 0";
  VLOG(2) << "Number of records of data: " << num_records;
  file_meta_data_.__set_num_rows(num_records);

  RowGroup row_group;
  row_group.__set_num_rows(num_records);
  vector<ColumnChunk> column_chunks;
  for (auto column_iter = file_columns_.begin() + 1 ;
       column_iter != file_columns_.end();
       ++column_iter) {
    auto column = *column_iter;
    if (column->Children().size() > 0) {
      VLOG(3) << "Skipping column: " << column->FullSchemaPath();
      continue;
    }
    VLOG(2) << "Writing column: " << column->FullSchemaPath();
    VLOG(2) << "\t" << column->ToString();
    VLOG(2) << "\t" << "Writing " << num_records << " records";
    column->Flush(fd_, protocol_.get());
    ColumnMetaData column_metadata = column->ParquetColumnMetaData();
    row_group.__set_total_byte_size(row_group.total_byte_size +
                                    column_metadata.total_uncompressed_size);
    VLOG(2) << "Wrote " << to_string(column_metadata.total_uncompressed_size)
            << " bytes for column: " << column->FullSchemaPath();
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
  VLOG(2) << "Done.";
  close(fd_);
}

void ParquetFile::SetSchema(ParquetColumn* root) {
  // Parquet's metadata needs the schema as a list, which results from
  // a depth-first traversal of the schema as a tree.
  vector<SchemaElement> parquet_schema_vector;
  ParquetColumnWalker* walker = new ParquetColumnWalker(&parquet_schema_vector);
  LOG_IF(WARNING, file_columns_.size() > 0)
    << "Internal file columns being reset";
  file_columns_.clear();
  DepthFirstSchemaTraversal(root, walker);
  VLOG(2) << root->ToString();

  file_meta_data_.__set_schema(parquet_schema_vector);
}

const ParquetColumn* ParquetFile::Root() const {
  return file_columns_.at(0);
}

ParquetColumnWalker::ParquetColumnWalker(vector<SchemaElement>* dfsVector)
    : dfsVector_(dfsVector) {
}

void ParquetColumnWalker::ColumnCallback(ParquetColumn* column) {
  SchemaElement schemaElement;
  schemaElement.__set_name(column->Name());
  schemaElement.__set_repetition_type(column->getFieldRepetitionType());
  // Parquet requires that we don't set the number of children if
  // the schema element is for a data column.
  if (column->Children().size() > 0) {
    schemaElement.__set_num_children(column->Children().size());
  } else {
    schemaElement.__set_type(column->getType());
  }
  dfsVector_->push_back(schemaElement);
}


}  // namespace parquet_file
