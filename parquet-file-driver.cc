#include "parquet-file.h"


int main(int argc, char* argv[]) {
  if (argc < 3) {
    printf("Specify filename and number of items:\n\n\t%s <output file> <NNN>\n\n", argv[0]);
    return 1;
  }

  parquet_file::ParquetFile output(argv[1]);
}
