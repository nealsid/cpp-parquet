/**
 * Copyright 2014 Mount Sinai School of Medicine.
 */
#include "parquet-file.h"

#include "parquet_types.h"
#include "thrift/protocol/TCompactProtocol.h"
#include "thrift/transport/TFDTransport.h"

#include <boost/shared_ptr.hpp>
#include <fcntl.h>
#include <string>

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

const char* kParquetMagicBytes = "PAR1";

namespace parquet_file {

  ParquetFile::ParquetFile(string file_base, int num_files) {
    assert(num_files == 1);
    ok_ = false;
    fd_ = open(file_base.c_str(), O_RDWR | O_CREAT | O_EXCL, 0700);
    if (fd_ == -1) {
      fprintf(stderr, "Could not create file %s: %s\n", 
	      file_base.c_str(), 
	      strerror(errno));
      return;
    }
    write(fd_, kParquetMagicBytes, strlen(kParquetMagicBytes));
    file_transport_.reset(new TFDTransport(fd_));
    protocol_.reset(new TCompactProtocol(file_transport_));
    ok_ = true;
    return;
  }

  ParquetColumn* ParquetFile::AddField(string column_name, 
				       Type data_type, 
				       FieldRepetitionType repetition_type) {

    return NULL;
  }

}  // namespace parquet_file
