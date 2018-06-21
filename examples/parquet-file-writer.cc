// A program that can write a Parquet file.  It's meant to be
// "explanatory" code, and not a well-structured program with suitable
// abstractions for use in other code.
//
// To compile, you need to download parquet-format, and have Thrift
// installed.  Run 
//
// $ thrift cpp parquet.thrift 
//
// in the directory containing Parquet.thrift to generate the needed
// source files.  Then, to build this program, run the following,
// adjusting paths to your local layout:
//
// $ g++ -o ./parquet-file-writer parquet-file-writer.cc -I../parquet-format/generated/gen-cpp -std=c++11 -lthrift ../parquet-format/generated/gen-cpp/parquet_types.cpp ../parquet-format/generated/gen-cpp/parquet_constants.cpp 
//
// You can run it like so:
//
// $ ./parquet-file-writer test.parquet 500
//
// Which will write 500 ints to a parquet file.  You can dump the schema if you have parquet-tools:
//
// $ ~/parquet-tools-0.1.0-SNAPSHOT/parquet-schema ./test.parquet
//
// TODO: provide a Makefile

#include "parquet_types.h"
#include "thrift/protocol/TCompactProtocol.h"
#include "thrift/transport/TFDTransport.h"
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

using std::string;

int main(int argc, char* argv[]) {
  if (argc < 3) {
    fprintf(stderr, "Specify output filename and number of rows on command line\n");
    return 1;
  }
  int fd = -1;
  fd = open(argv[1], O_RDWR | O_CREAT | O_EXCL, 0700);
  if (fd == -1) {
    fprintf(stderr, "Could not create file %s: %s\n", argv[1], strerror(errno));
    return 1;
  }
  int num_rows = atoi(argv[2]);
  string header = "PAR1";
  write(fd, header.c_str(), 4);

  off_t column_header_offset = lseek(fd, 0, SEEK_CUR);
  fprintf(stderr, "Offset of first column page header: %lld\n", column_header_offset);

  // Think of a Transport as a place to stream data to. In our case,
  // we're using a file-descriptor transport.
  std::shared_ptr<TFDTransport> file_transport(new TFDTransport(fd));
  // Protocol objects take a transport object.  Data is encoded and
  // then written to/read from the transport object.  Parquet
  // specifies that various metadata records are encoded using the
  // ThriftCompactProtocol, and the Thrift library provides classes to
  // write various pieces of data in that protocol to a transport.
  TCompactProtocol* protocol = new TCompactProtocol(file_transport);

  PageHeader page_header;
  page_header.__set_type(PageType::DATA_PAGE);
  page_header.__set_uncompressed_page_size(4 + (sizeof(int) * num_rows));
  page_header.__set_compressed_page_size(4 + (sizeof(int) * num_rows));
  DataPageHeader data_header;
  data_header.__set_num_values(num_rows);
  data_header.__set_encoding(Encoding::PLAIN);
  data_header.__set_definition_level_encoding(Encoding::RLE);
  data_header.__set_repetition_level_encoding(Encoding::RLE);
  page_header.__set_data_page_header(data_header);
  uint32_t page_header_length = page_header.write(protocol);

  // We don't write definition levels, because the field is required.
  // We don't write repetition levels for singular fields.

  // Write the data after the column/page header.  A column can have
  // many pages, but not the other way around.
  int data[num_rows];
  for (int i = 0; i < num_rows; ++i) {
    data[i] = i;
  }
  for (int i = 0; i < num_rows; ++i) {
    ssize_t written = write(fd, data + i, 4);
    if (written != 4) {
      fprintf(stderr, "Did not write 4 bytes for element %d\n", i);
    }
  }

  // File Metadata contains the schema, and then RowGroups.  Each
  // RowGroup contain ColumnChunks.  Each ColumnChunk contains
  // ColumnMetaData.  The ColumnChunk points to the PageHeader
  // (created above) and the ColumnMetadata contains
  // encoding/compression/schema information (like which column in the
  // schema it is)
  FileMetaData file_metadata;
  file_metadata.__set_num_rows(num_rows);
  file_metadata.__set_version(1);
  file_metadata.__set_created_by("Neal sid");
  SchemaElement one_column, root_column;

  // There is always one "container" column that contains all the real
  // columns of data, which we call root_column.
  root_column.__set_name("root");
  root_column.__set_num_children(1);
  // Strangely, you cannot set num_children to 0 for leaf nodes,
  // parquet-tools will throw an error about it being invalid.  It
  // must be unset.
  one_column.__set_type(Type::INT32);
  one_column.__set_repetition_type(FieldRepetitionType::REQUIRED);
  one_column.__set_name("randomints");
  // A schema is flattened by a depth first traversal and the
  // resulting list is what's stored here.  Num children is set on
  // each node to determine which subsequent nodes are children vs. a
  // sibling at the current nesting level.
  file_metadata.__set_schema({root_column, one_column});

  RowGroup row_group;
  row_group.__set_num_rows(num_rows);
  row_group.__set_total_byte_size(num_rows * sizeof(int));
  
  ColumnMetaData column_metadata;
  column_metadata.__set_type(Type::INT32);
  column_metadata.__set_encodings({Encoding::PLAIN});
  column_metadata.__set_path_in_schema({"randomints"});
  column_metadata.__set_codec(CompressionCodec::UNCOMPRESSED);
  column_metadata.__set_num_values(num_rows);
  column_metadata.__set_total_uncompressed_size(num_rows * sizeof(int));
  column_metadata.__set_total_compressed_size(num_rows * sizeof(int));
  column_metadata.__set_data_page_offset(column_header_offset);

  ColumnChunk column_chunk;
  column_chunk.__set_file_path(argv[1]);
  column_chunk.__set_file_offset(column_header_offset);
  column_chunk.__set_meta_data(column_metadata);

  row_group.__set_columns({column_chunk});
  file_metadata.__set_row_groups({row_group});

  uint32_t length = file_metadata.write(protocol);
  fprintf(stderr, "Footer length: %u\n", length);
  write(fd, &length, 4);
  write(fd, header.c_str(), 4);
}
