#include <glog/logging.h>
#include <gtest/gtest.h>
#include <parquet-file/parquet-file.h>
#include <parquet-file/parquet-column.h>
#include <unistd.h>

using parquet_file::ParquetColumn;
using parquet_file::ParquetFile;

namespace {

// The fixture for testing class Foo.
class ParquetFileTest : public ::testing::Test {
 protected:
  // You can remove any or all of the following functions if its body
  // is empty.

  ParquetFileTest() {
    // You can do set-up work for each test here.
    strcpy(template_, "/tmp/parquetFileTmp.XXXXXX");
  }

  virtual ~ParquetFileTest() {
    // You can do clean-up work that doesn't throw exceptions here.
  }

  // If the constructor and destructor are not enough for setting up
  // and cleaning up each test, you can define the following methods:

  virtual void SetUp() {
    // Code here will be called immediately after the constructor (right
    // before each test).
    LOG(INFO) << "Assigning filename";
    output_filename_.assign(mktemp(template_));
  }

  virtual void TearDown() {
    // Code here will be called immediately after each test (right
    // before the destructor).
  }

  // Objects declared here can be used by all tests in the test case for Foo.
  string output_filename_;
  char template_[32];
};


// Tests that the output works with two columns of required integers.
TEST_F(ParquetFileTest, TwoColumnRequiredInts) {
  LOG(INFO) << output_filename_;
  ParquetFile output(output_filename_);

  ParquetColumn* one_column = 
    new ParquetColumn({"AllInts"}, parquet::Type::INT32, 
		      1,
		      FieldRepetitionType::REQUIRED, 
		      Encoding::PLAIN,
		      CompressionCodec::UNCOMPRESSED);

  ParquetColumn* two_column = 
    new ParquetColumn({"AllInts1"}, parquet::Type::INT32, 
		      1,
		      FieldRepetitionType::REQUIRED, 
		      Encoding::PLAIN,
		      CompressionCodec::UNCOMPRESSED);

  ParquetColumn* root_column = 
    new ParquetColumn({"root"}, parquet::Type::INT32, 
		      0,
		      FieldRepetitionType::REQUIRED, 
		      Encoding::PLAIN,
		      CompressionCodec::UNCOMPRESSED);
  root_column->SetChildren({one_column, two_column});
  output.SetSchema(root_column);
  uint32_t data[500];
  for (int i = 0; i < 500; ++i) {
    data[i] = i;
  }
  one_column->AddRows(data, 0, 500);
  for (int i = 0; i < 500; ++i) {
    data[i] = i;
  }
  two_column->AddRows(data, 0, 500);
  output.Flush();
}

}  // namespace

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  google::InitGoogleLogging(argv[0]);
  return RUN_ALL_TESTS();
}
