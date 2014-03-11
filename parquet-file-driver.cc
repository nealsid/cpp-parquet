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
  output.SetSchema({one_column});
  output.Flush();
}
