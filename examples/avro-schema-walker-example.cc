#include <avro-schema/avro-schema-walker.h>
#include <glog/logging.h>
#include <parquet-file/parquet-column.h>
#include <parquet-file/parquet-file.h>

using parquet_file::AvroSchemaWalker;
using parquet_file::AvroSchemaToParquetSchemaConverter;
using parquet_file::ParquetColumn;
using parquet_file::ParquetFile;

int main(int argc, char* argv[]) {
  google::InitGoogleLogging(argv[0]);
  if (argc < 2) {
    LOG(FATAL) <<
      "Specify JSON schema file on command line";
    return 1;
  }
  AvroSchemaWalker walker(argv[1]);
  AvroSchemaToParquetSchemaConverter* converter = new AvroSchemaToParquetSchemaConverter();
  walker.WalkSchema(converter);
  ParquetColumn* root = converter->Root();
  ParquetFile* parquet_file = new ParquetFile("test.parquet");
  parquet_file->SetSchema(root);
  parquet_file->Flush();
}
