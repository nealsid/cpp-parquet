#include "parquet-file.h"
#include <glog/logging.h>

using parquet_file::ParquetColumn;
using parquet_file::ParquetFile;

int main(int argc, char* argv[]) {
  if (argc < 3) {
    printf("Specify filename and number of items:\n\n\t%s <output file> <NNN>\n\n", argv[0]);
    return 1;
  }
  google::InitGoogleLogging(argv[0]);

  ParquetFile output(argv[1]);
  ParquetColumn* one_column = 
    new ParquetColumn("AllInts", parquet::Type::INT32, 
		      FieldRepetitionType::REQUIRED, 
		      Encoding::PLAIN,
		      CompressionCodec::UNCOMPRESSED);
  ParquetColumn* two_column = 
    new ParquetColumn("AllInts1", parquet::Type::INT32, 
		      FieldRepetitionType::REQUIRED, 
		      Encoding::PLAIN,
		      CompressionCodec::UNCOMPRESSED);
  output.SetSchema({one_column, two_column});
  uint32_t data[500];
  for (int i = 0; i < 500; ++i) {
    data[i] = i;
  }
  one_column->AddRows(data, 500);
  for (int i = 0; i < 500; ++i) {
    data[i] = 500 + i;
  }
  two_column->AddRows(data, 500);
  output.Flush();
}
