// Copyright 2014 Mount Sinai School of Medicine.

#include "parquet-file.h"

#include <boost/shared_ptr.hpp>
#include <fcntl.h>
#include <glog/logging.h>
#include <parquet_types.h>
#include <string>
#include <thrift/protocol/TCompactProtocol.h>
#include <thrift/transport/TFDTransport.h>

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

const char* kParquetMagicBytes = "PAR1";

namespace parquet_file {

ParquetFile::ParquetFile(string file_base, int num_files) {
  // TODO: remove this restrction
  assert(num_files == 1);

  ok_ = false;

  fd_ = open(file_base.c_str(), O_RDWR | O_CREAT | O_EXCL, 0700);
  if (fd_ == -1) {
    fprintf(stderr, "Could not create file %s: %s\n", 
            file_base.c_str(), 
            strerror(errno));
    return;
  }

  // Write magic header
  write(fd_, kParquetMagicBytes, strlen(kParquetMagicBytes));

  // Construct transports (provided by Thrift) for writing to the
  // file.
  file_transport_.reset(new TFDTransport(fd_));
  protocol_.reset(new TCompactProtocol(file_transport_));

  // Parquet-specific metadata for the file.
  file_meta_data_.__set_num_rows(num_rows);
  file_meta_data_.__set_version(1);
  file_meta_data_.__set_created_by("Neal sid");

  ok_ = true;
  return;
}

void ParquetFile::DepthFirstSchemaTraversal(const ParquetColumn& root_column,
                                            ParquetColumnWalker* callback) {
  callback->ColumnCallback(root_column);
  const vector<ParquetColumn*>& children = root_column.Children();
  for (ParquetColumn* c : children) {
    DepthFirstSchemaTraversal(root_column, callback);
  }
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
  for (auto column : schema) {
    VLOG(2) << column->ToString();
    SchemaElement one_column;
    one_column.__set_name(column->Name());
    one_column.__set_repetition_type(column->RepetitionType());
    assert(column->Children().size() == 0);
  }
  file_meta_data_.__set_schema({root_column});
}

}  // namespace parquet_file
