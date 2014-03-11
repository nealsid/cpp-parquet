#include "parquet-file.h"
#include <glog/logging.h>

using parquet_file::ParquetDataColumn;
using parquet_file::ParquetFile;

int main(int argc, char* argv[]) {
  if (argc < 3) {
    printf("Specify filename and number of items:\n\n\t%s <output file> <NNN>\n\n", argv[0]);
    return 1;
  }
  google::InitGoogleLogging(argv[0]);

  ParquetFile output(argv[1]);
  ParquetDataColumn* one_column = new ParquetDataColumn("AllInts", parquet::Type::INT32, FieldRepetitionType::REQUIRED);
  output.SetSchema({one_column});
}
