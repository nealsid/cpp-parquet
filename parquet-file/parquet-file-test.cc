// Copyright 2014 Mount Sinai School of Medicine

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
    snprintf(template_, sizeof(template_), "/tmp/parquetFileTmp.XXXXXX");
  }

  virtual ~ParquetFileTest() {
    // You can do clean-up work that doesn't throw exceptions here.
  }

  // If the constructor and destructor are not enough for setting up
  // and cleaning up each test, you can define the following methods:

  virtual void SetUp() {
    // Code here will be called immediately after the constructor (right
    // before each test).
    output_filename_.assign(mktemp(template_));
    LOG(INFO) << "Assigning filename: " << output_filename_;
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
  ParquetFile output(output_filename_);

  ParquetColumn* one_column =
      new ParquetColumn({"AllInts"}, parquet::Type::INT32,
                        1, 1,
                        FieldRepetitionType::REQUIRED,
                        Encoding::PLAIN,
                        CompressionCodec::UNCOMPRESSED);

  ParquetColumn* two_column =
      new ParquetColumn({"AllInts1"}, parquet::Type::INT32,
                        1, 1,
                        FieldRepetitionType::REQUIRED,
                        Encoding::PLAIN,
                        CompressionCodec::UNCOMPRESSED);

  ParquetColumn* root_column =
      new ParquetColumn({"root"}, FieldRepetitionType::REQUIRED);
  root_column->SetChildren({one_column, two_column});
  output.SetSchema(root_column);
  uint32_t data[500];
  for (int i = 0; i < 500; ++i) {
    data[i] = i;
  }
  one_column->AddRecords(data, 0, 500);
  for (int i = 0; i < 500; ++i) {
    data[i] = i;
  }
  two_column->AddRecords(data, 0, 500);
  output.Flush();
}

// Tests that the output works with one column of 96 byte ints.
TEST_F(ParquetFileTest, OneColumn96ByteInts) {
  ParquetFile output(output_filename_);

  ParquetColumn* one_column =
      new ParquetColumn({"AllInts"}, parquet::Type::INT96,
                        1, 1,
                        FieldRepetitionType::REQUIRED,
                        Encoding::PLAIN,
                        CompressionCodec::UNCOMPRESSED);

  ParquetColumn* root_column =
      new ParquetColumn({"root"}, FieldRepetitionType::REQUIRED);
  root_column->SetChildren({one_column});
  output.SetSchema(root_column);
  uint8_t data[12];
  for (int i = 0; i < 500; ++i) {
    data = i;
    one_column->AddRecords(data, 0, 500);
  }
  for (int i = 0; i < 500; ++i) {
    data[i] = i;
  }
  two_column->AddRecords(data, 0, 500);
  output.Flush();
}

// Tests that the output works with two columns of integers, one array
// and one non-array.  The array column has 1 array of 500 integers
// the other column has 1 individual integer in the records.
TEST_F(ParquetFileTest, TwoColumnOfIntsOneRepeated) {
  ParquetFile output(output_filename_);

  ParquetColumn* root_column =
      new ParquetColumn({"root"}, FieldRepetitionType::REQUIRED);

  ParquetColumn* repeated_column =
      new ParquetColumn({"AllIntsRepeated"}, parquet::Type::INT32,
                        1, 1,
                        FieldRepetitionType::REPEATED,
                        Encoding::PLAIN,
                        CompressionCodec::UNCOMPRESSED);

  ParquetColumn* required_column =
      new ParquetColumn({"AllIntsRequired"}, parquet::Type::INT32,
                        1, 1,
                        FieldRepetitionType::REQUIRED,
                        Encoding::PLAIN,
                        CompressionCodec::UNCOMPRESSED);

  root_column->SetChildren({repeated_column, required_column});
  output.SetSchema(root_column);
  uint32_t data[500];
  for (int i = 0; i < 500; ++i) {
    data[i] = i;
  }
  repeated_column->AddRepeatedData(data, 0, 500);
  required_column->AddRecords(data, 0, 1);
  output.Flush();
}

// Tests that the output works with one column of array integers.  The
// test has 250 records of 2 element arrays.
TEST_F(ParquetFileTest, OneColumn250Records) {
  ParquetFile output(output_filename_);

  ParquetColumn* root_column =
      new ParquetColumn({"root"}, FieldRepetitionType::REQUIRED);

  ParquetColumn* repeated_column =
      new ParquetColumn({"AllIntsRepeated"}, parquet::Type::INT32,
                        1, 1,
                        FieldRepetitionType::REPEATED,
                        Encoding::PLAIN,
                        CompressionCodec::UNCOMPRESSED);

  root_column->SetChildren({repeated_column});
  output.SetSchema(root_column);
  uint32_t data[500];
  for (int i = 0; i < 500; ++i) {
    data[i] = i;
  }
  for (int i = 0; i < 250; i += 2) {
    repeated_column->AddRepeatedData(data + i, 0, 2);
  }
  output.Flush();
}

// Tests that the output works with two columns of integers, one array
// and one non-array.  The array column has 1 array of 4 integers and
// 1 individual integer, for a total of 2 different records.  The
// other column has 2 individual integers in the records.
TEST_F(ParquetFileTest, TwoColumnOfIntsOneRepeatedAndNonRepeatedData) {
  ParquetFile output(output_filename_);

  ParquetColumn* root_column =
      new ParquetColumn({"root"}, FieldRepetitionType::REQUIRED);

  ParquetColumn* repeated_column =
      new ParquetColumn({"AllIntsRepeated"}, parquet::Type::INT32,
                        1, 1,
                        FieldRepetitionType::REPEATED,
                        Encoding::PLAIN,
                        CompressionCodec::UNCOMPRESSED);

  ParquetColumn* required_column =
      new ParquetColumn({"AllIntsRequired"}, parquet::Type::INT32,
                        1, 1,
                        FieldRepetitionType::REQUIRED,
                        Encoding::PLAIN,
                        CompressionCodec::UNCOMPRESSED);

  root_column->SetChildren({repeated_column, required_column});
  output.SetSchema(root_column);
  uint32_t data[5];
  for (int i = 0; i < 5; ++i) {
    data[i] = i;
  }
  repeated_column->AddRepeatedData(data, 0, 4);
  repeated_column->AddRecords(data + 4, 0, 1);

  required_column->AddRecords(data, 0, 2);
  output.Flush();
}

// Tests that the output works with optional data even if all data is
// filled in.
TEST_F(ParquetFileTest, OneColumnOptionalData) {
  ParquetFile output(output_filename_);


  ParquetColumn* root_column =
      new ParquetColumn({"root"}, FieldRepetitionType::REQUIRED);

  ParquetColumn* optional_column =
      new ParquetColumn({"OptionalInts"}, parquet::Type::INT32,
                        1, 1,
                        FieldRepetitionType::OPTIONAL,
                        Encoding::PLAIN,
                        CompressionCodec::UNCOMPRESSED);

  root_column->SetChildren({optional_column});
  output.SetSchema(root_column);
  uint32_t data[5];
  for (int i = 0; i < 5; ++i) {
    data[i] = 50 * i;
  }
  for (int i = 0; i < 5; ++i) {
    optional_column->AddRecords(data + i, 0, 1);
  }
  output.Flush();
}

// Tests that the output works with optional data with nulls
TEST_F(ParquetFileTest, OneColumn500Nulls) {
  ParquetFile output(output_filename_);

  ParquetColumn* root_column =
      new ParquetColumn({"root"}, FieldRepetitionType::REQUIRED);

  ParquetColumn* optional_column =
      new ParquetColumn({"OptionalInts"}, parquet::Type::INT32,
                        1, 1,
                        FieldRepetitionType::OPTIONAL,
                        Encoding::PLAIN,
                        CompressionCodec::UNCOMPRESSED);

  root_column->SetChildren({optional_column});
  output.SetSchema(root_column);
  optional_column->AddNulls(0, 0, 500);
  output.Flush();
}

// Tests that the output works with optional data with interspersed
// nulls & data.
TEST_F(ParquetFileTest, OneColumn500NullsAndData) {
  ParquetFile output(output_filename_);

  ParquetColumn* root_column =
      new ParquetColumn({"root"}, FieldRepetitionType::REQUIRED);

  ParquetColumn* optional_column =
      new ParquetColumn({"OptionalInts"}, parquet::Type::INT32,
                        1, 1,
                        FieldRepetitionType::OPTIONAL,
                        Encoding::PLAIN,
                        CompressionCodec::UNCOMPRESSED);

  root_column->SetChildren({optional_column});
  output.SetSchema(root_column);
  uint32_t data[500];
  for (int i = 0; i < 500; ++i) {
    data[i] = i;
  }
  for (int i = 0; i < 500; ++i) {
    optional_column->AddNulls(0, 0, 1);
    optional_column->AddRecords(data + i, 0, 1);
  }
  output.Flush();
}

// Tests that the output works with nested fields.
TEST_F(ParquetFileTest, OneColumnNestedData) {
  ParquetFile output(output_filename_);

  ParquetColumn* root_column =
      new ParquetColumn({"root"}, FieldRepetitionType::REQUIRED);

  ParquetColumn* new_column = nullptr;
  ParquetColumn* old_column = root_column;
  vector<string> schema_path;
  for (int i = 1; i <= 50; ++i) {
    schema_path.push_back("OptionalInts" + to_string(i));
    if (i == 50) {
      new_column =
          new ParquetColumn(schema_path, parquet::Type::INT32,
                            1, 1,
                            FieldRepetitionType::REQUIRED,
                            Encoding::PLAIN,
                            CompressionCodec::UNCOMPRESSED);
    } else {
      new_column =
          new ParquetColumn(schema_path,
                            FieldRepetitionType::REQUIRED);
    }
    old_column->SetChildren({new_column});
    old_column = new_column;
  }

  output.SetSchema(root_column);
  uint32_t data[500];
  for (int i = 0; i < 500; ++i) {
    data[i] = i;
  }
  for (int i = 0; i < 500; ++i) {
    old_column->AddRecords(data + i, 0, 1);
  }
  output.Flush();
}

// Tests that the output works with nested optional fields at the
// bottom of the schema tree (i.e. innermost field)
TEST_F(ParquetFileTest, OneColumnNestedOptionalData) {
  ParquetFile output(output_filename_);

  ParquetColumn* root_column =
      new ParquetColumn({"root"}, FieldRepetitionType::REQUIRED);

  ParquetColumn* new_column = nullptr;
  ParquetColumn* old_column = root_column;
  vector<string> schema_path;
  for (int i = 1; i <= 50; ++i) {
    schema_path.push_back("OptionalInts" + to_string(i));
    if (i == 50) {
      new_column =
          new ParquetColumn(schema_path, parquet::Type::INT32,
                            1, 1,
                            FieldRepetitionType::OPTIONAL,
                            Encoding::PLAIN,
                            CompressionCodec::UNCOMPRESSED);
    } else {
      new_column =
          new ParquetColumn(schema_path,
                            FieldRepetitionType::REQUIRED);
    }
    old_column->SetChildren({new_column});
    old_column = new_column;
  }

  output.SetSchema(root_column);
  uint32_t data[250];
  for (int i = 0; i < 250; ++i) {
    data[i] = i;
  }
  for (int i = 0; i < 250; ++i) {
    old_column->AddRecords(data + i, 0, 1);
    old_column->AddNulls(0, 0, 1);
  }
  output.Flush();
}


}  // namespace

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  google::InitGoogleLogging(argv[0]);
  return RUN_ALL_TESTS();
}
