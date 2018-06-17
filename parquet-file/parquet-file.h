// Copyright 2014 Mount Sinai School of Medicine

#include "./parquet_types.h"

#include <fcntl.h>
#include <glog/logging.h>
#include <parquet-file/parquet-column.h>
#include <thrift/protocol/TCompactProtocol.h>
#include <thrift/transport/TFDTransport.h>

#include <set>
#include <string>
#include <vector>

#ifndef PARQUET_FILE_PARQUET_FILE_H_
#define PARQUET_FILE_PARQUET_FILE_H_

using apache::thrift::transport::TFDTransport;
using apache::thrift::protocol::TCompactProtocol;
using parquet::CompressionCodec;
using parquet::FileMetaData;
using parquet::SchemaElement;
using std::set;
using std::string;
using std::vector;

const uint32_t kDataBytesPerPage = 81920000;

namespace parquet_file {
const int kMaxDataBytesPerRowGroup = 1024000;

class ParquetColumnWalker;
// Main class that represents a Parquet file on disk.
class ParquetFile {
 public:
  // Constructor.  file_base is the output file. The num_files is a
  // sharding parameter, but currently isn't supported.
  ParquetFile(string file_base, int num_files = 1);

  // Set the schema of this file.
  void SetSchema(ParquetColumn* root);
  // Return the root of the schema.
  const ParquetColumn* Root() const;

  // Flush the file to the filename given in the constructor.
  void Flush();
  // Close the file.
  void Close();
  bool IsOK() { return ok_; }

  uint64_t NumberOfRecords() const;

  // Calculates the number of rowgroups for the data in this Parquet
  // file.  It isn't quite correct because it only looks at data, not
  // the encoded R&D levels, but this will be rectified later.
  uint32_t CalculateNumberOfRowGroups() const;

  uint64_t BytesForRecord(uint64_t record_index) const;
 private:
  // Walker for the schema.  Parquet requires columns specified as a
  // vector that is the depth first preorder traversal of the schema,
  // which is what this method does.
  void DepthFirstSchemaTraversal(ParquetColumn* root_column,
                                 ParquetColumnWalker* callback);


  // Fills the passed in set with the number of rows in all
  // data-containing columns.
  void NumberOfRecords(set<uint64_t>* column_record_counts) const;

  // A vector representing the DFS traversal of the columns.
  vector<ParquetColumn*> file_columns_;

  // Parquet Thrift structure that has metadata about the entire file.
  FileMetaData file_meta_data_;

  // Variables that represent file system location and data.
  string file_base_;
  int num_files_;
  int fd_;

  // Member variables used to actually encode & write the data to
  // disk.
  std::shared_ptr<TFDTransport> file_transport_;
  std::shared_ptr<TCompactProtocol> protocol_;

  // A bit indicating that we've initialized OK, defined the schema,
  // and are ready to start accepting & writing data.
  bool ok_;
};

// A callback class for use with DepthFirstSchemaTraversal.  The
// callback is called for each column in a depth-first, preorder,
// traversal.
class ParquetColumnWalker {
 public:
  // A vector in which nodes are appended according to their order in
  // the depth first traversal.  We do not take ownership of the
  // vector.
  explicit ParquetColumnWalker(vector<SchemaElement>* dfsVector);

  // Override this, and it will be executed for each column.
  void ColumnCallback(ParquetColumn* column);

 private:
  // SchemaElement is a POD object, which is why we store the actual
  // message and not a pointer.
  vector<SchemaElement>* dfsVector_;
};

}  // namespace parquet_file

#endif  // PARQUET_FILE_PARQUET_FILE_H_
