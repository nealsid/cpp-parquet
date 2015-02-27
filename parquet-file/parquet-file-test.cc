// Copyright 2014 Mount Sinai School of Medicine

#include <parquet-file/parquet-file.h>

#include <algorithm>
#include <glog/logging.h>
#include <gtest/gtest.h>
#include <limits.h>
#include <parquet-file/parquet-column.h>
#include <stdint.h>
#include <strings.h>
#include <unistd.h>

using parquet_file::ParquetColumn;
using parquet_file::ParquetFile;
using parquet_file::RecordMetadata;

namespace parquet_file {

// The fixture for testing class ParquetFile.
class ParquetFileTest : public ::testing::Test {
 protected:
  ParquetFileTest() {
    snprintf(template_, sizeof(template_), "/tmp/parquetFileTmp.XXXXXX");
    char* parquet_dump_path = getenv("PARQUET_DUMP_PATH");
    if (parquet_dump_path) {
      parquet_dump_executable_path_.assign(parquet_dump_path);
      VLOG(2) << "Using " << parquet_dump_executable_path_ << " to validate files";
    }
  }

  virtual void SetUp() {
    output_filename_.assign(mktemp(template_));
    VLOG(2) << "Assigning filename: " << output_filename_;
  }

  virtual void TearDown() {
    if (!parquet_dump_executable_path_.empty()) {
      const ::testing::TestInfo* const test_info =
          ::testing::UnitTest::GetInstance()->current_test_info();
      char golden_filename[1024];
      snprintf(golden_filename, 1024, "%s-golden", test_info->name());
      string golden_filename_no_slashes(golden_filename);
      std::replace(golden_filename_no_slashes.begin(), golden_filename_no_slashes.end(), '/','-');
      char command_line[1024];
      snprintf(command_line, 1024, "%s %s > %s", parquet_dump_executable_path_.c_str(), output_filename_.c_str(), golden_filename_no_slashes.c_str());
      system(command_line);
    }

  }

  void CheckRecordMetadata(const ParquetFile& output,
                           const int number_of_records,
                           const vector<uint64_t>& record_sizes) const {
    CHECK_EQ(output.NumberOfRecords(), number_of_records) <<
        "Number of records was not as expected";
    int record_size_index = 0;
    for (int i = 0; i < number_of_records; ++i) {
      uint64_t bytesForRecord = output.BytesForRecord(i);
      VLOG(3) << "\tRecord " << i << " size: " << bytesForRecord;
      CHECK_LE(record_size_index, record_sizes.size());
      if (record_size_index == record_sizes.size()) {
        record_size_index = 0;
      }
      CHECK_EQ(bytesForRecord, record_sizes[record_size_index]) <<
          "Record size was not correct";
      ++record_size_index;
    }
  }
  // Objects declared here can be used by all tests in the test case for Foo.
  string output_filename_;
  char template_[32];
  string parquet_dump_executable_path_;
};

class ParquetFileBasicRequiredTest :
      public ParquetFileTest,
      public ::testing::WithParamInterface<parquet::Type::type> {
 protected:

  // This class is a friend of ParquetColumn but the test cases (which
  // are subclasses) are not. So they can call this method to get &
  // examine the record metadata.
  vector<RecordMetadata>& getColumnRecordMetadata(ParquetColumn* column) {
    return column->record_metadata;
  }

  uint8_t* SentinelValueForType() {
    uint8_t* val_ptr = new uint8_t[ParquetColumn::BytesForDataType(GetParam())];
    switch(GetParam()) {
      case parquet::Type::INT32:
        *(int32_t*)val_ptr = INT_MAX;
        break;
      case parquet::Type::INT64:
        *(int64_t*)val_ptr = INT64_MAX;
        break;
      case parquet::Type::FLOAT:
        *(float*)val_ptr = std::numeric_limits<float>::max();
        break;
      case parquet::Type::DOUBLE:
        *(double*)val_ptr = std::numeric_limits<double>::max();
        break;
      default:
        LOG(FATAL) << "Invalid type specified in SentinelValueForType()" << GetParam();
    }
    return val_ptr;
  }
};

// Tests that the output works with two columns of required integers.
TEST_P(ParquetFileBasicRequiredTest, TwoRequiredColumns) {
  ParquetFile output(output_filename_);

  parquet::Type::type column_type = GetParam();
  ParquetColumn* one_column =
    new ParquetColumn({"AllInts"}, column_type,
                      1, 1,
                      FieldRepetitionType::REQUIRED,
                      Encoding::PLAIN,
                      CompressionCodec::UNCOMPRESSED);

  ParquetColumn* two_column =
    new ParquetColumn({"AllInts1"}, column_type,
                      1, 1,
                      FieldRepetitionType::REQUIRED,
                      Encoding::PLAIN,
                      CompressionCodec::UNCOMPRESSED);

  ParquetColumn* root_column =
    new ParquetColumn({"root"}, FieldRepetitionType::REQUIRED);
  root_column->SetChildren({one_column, two_column});
  output.SetSchema(root_column);
  std::unique_ptr<uint8_t> data_value(SentinelValueForType());

  int num_values = 500;
  for (int i = 0; i < num_values; ++i) {
    one_column->AddRecords(data_value.get(), 0, 1);
    two_column->AddRecords(data_value.get(), 0, 1);
  }
  output.Flush();
  uint64_t expected_bytes_for_each_record = 2 * ParquetColumn::BytesForDataType(GetParam());
  CheckRecordMetadata(output,
                      num_values,
                      { expected_bytes_for_each_record });
}

INSTANTIATE_TEST_CASE_P(ParquetFileBasicTest,
                        ParquetFileBasicRequiredTest,
                        ::testing::Values(parquet::Type::INT32,
                                          parquet::Type::INT64,
                                          parquet::Type::DOUBLE,
                                          parquet::Type::FLOAT));

// Tests that the output works with two columns of required integers.
TEST_F(ParquetFileTest, TwoRequiredColumnsWithProvidedBuffer) {
  ParquetFile output(output_filename_);

  parquet::Type::type column_type = parquet::Type::INT32;
  boost::shared_array<uint8_t> buffer1(new uint8_t[2000]);
  boost::shared_array<uint8_t> buffer2(new uint8_t[2000]);
  bzero(buffer1.get(), 2000);
  bzero(buffer2.get(), 2000);
  ParquetColumn* one_column =
    new ParquetColumn({"AllInts"}, column_type,
                      1, 1,
                      FieldRepetitionType::REQUIRED,
                      Encoding::PLAIN,
                      CompressionCodec::UNCOMPRESSED,
                      buffer1,
                      2000);

  ParquetColumn* two_column =
    new ParquetColumn({"AllInts1"}, column_type,
                      1, 1,
                      FieldRepetitionType::REQUIRED,
                      Encoding::PLAIN,
                      CompressionCodec::UNCOMPRESSED,
                      buffer2,
                      2000);

  ParquetColumn* root_column =
    new ParquetColumn({"root"}, FieldRepetitionType::REQUIRED);
  root_column->SetChildren({one_column, two_column});
  output.SetSchema(root_column);
  int32_t data_value = INT_MAX;

  int num_values = 500;
  for (int i = 0; i < num_values; ++i) {
    one_column->AddRecords(&data_value, 0, 1);
    two_column->AddRecords(&data_value, 0, 1);
  }
  output.Flush();
  // Verify that the Parquet code actually used our buffers.
  for (int i = 0 ; i < 500 ; ++i) {
    CHECK_EQ(*(((int32_t*)buffer1.get() + i)), INT_MAX) <<
        "Buffer was not filled with UINT_MAX";
    CHECK_EQ(*(((int32_t*)buffer2.get() + i)), INT_MAX) <<
        "Buffer was not filled with UINT_MAX";
  }
  uint64_t expected_bytes_for_each_record = 2 * ParquetColumn::BytesForDataType(parquet::Type::INT32);
  CheckRecordMetadata(output,
                      num_values,
                      { expected_bytes_for_each_record });
}

// Tests that the output works with one column of required integers
// that end up being 2 gibibytes.
// TEST_F(ParquetFileTest, OneRequiredColumnsTwoGibibytesOfData) {
//   ParquetFile output(output_filename_);
//   boost::shared_array<uint8_t> buffer1(new uint8_t[2147483648]);

//   parquet::Type::type column_type = parquet::Type::INT32;
//   ParquetColumn* one_column =
//     new ParquetColumn({"AllInts"}, column_type,
//                       1, 1,
//                       FieldRepetitionType::REQUIRED,
//                       Encoding::PLAIN,
//                       CompressionCodec::UNCOMPRESSED,
//                       buffer1,
//                       2147483648);

//   ParquetColumn* root_column =
//     new ParquetColumn({"root"}, FieldRepetitionType::REQUIRED);
//   root_column->SetChildren({one_column});
//   output.SetSchema(root_column);
//   int32_t data_value = INT_MAX;

//   int num_values = 2147483648 / 4;
//   for (int i = 0; i < num_values; ++i) {
//     one_column->AddRecords(&data_value, 0, 1);
//   }
//   output.Flush();
// }

class RowGroupTest : public ParquetFileTest {

};
// Tests that the row group calculcations are correct, but does not
// actually flush and verify the file at this time.
TEST_F(RowGroupTest, OneRequiredColumnTwoGibibytesOfData) {
  ParquetFile output(output_filename_);
  boost::shared_array<uint8_t> buffer1(new uint8_t[2147483648]);

  parquet::Type::type column_type = parquet::Type::INT32;
  ParquetColumn* one_column =
    new ParquetColumn({"AllInts"}, column_type,
                      1, 1,
                      FieldRepetitionType::REQUIRED,
                      Encoding::PLAIN,
                      CompressionCodec::UNCOMPRESSED,
                      buffer1,
                      2147483648);

  ParquetColumn* root_column =
    new ParquetColumn({"root"}, FieldRepetitionType::REQUIRED);
  root_column->SetChildren({one_column});
  output.SetSchema(root_column);
  int32_t data_value = INT_MAX;

  int num_values = 2147483648 / 4;
  one_column->AddSingletonValueAsNRecords(&data_value, 0, num_values);

  // Intentionally skipping flush for this test case.
  CHECK_EQ(output.CalculateNumberOfRowGroups(),
           2147483648 / parquet_file::kMaxDataBytesPerRowGroup)
      << "Number of row groups was not as expected";
}

// Tests that the output works with two columns of integers, one array
// and one non-array.  The array column has 1 array of 500 integers
// the other column has 1 individual integer in the record.
TEST_F(ParquetFileTest, TwoColumnsOfIntsOneRepeated) {
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
  uint64_t expected_bytes_for_each_record = 2004;
  CheckRecordMetadata(output,
                      1,
                      { expected_bytes_for_each_record });
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
  int num_records = 250;
  for (int i = 0; i < num_records; i += 2) {
    repeated_column->AddRepeatedData(data + i, 0, 2);
  }
  output.Flush();

  uint64_t expected_bytes_for_each_record =
      2 * ParquetColumn::BytesForDataType(parquet::Type::INT32);
  CheckRecordMetadata(output,
                      num_records / 2,
                      { expected_bytes_for_each_record });
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
  uint64_t bytesForRecord = output.BytesForRecord(0);
  VLOG(3) << "\tRecord 0 size: " << bytesForRecord;
  CHECK_EQ(bytesForRecord, 20) <<
      "Record size was not correct";

  bytesForRecord = output.BytesForRecord(1);
  VLOG(3) << "\tRecord 1 size: " << bytesForRecord;
  CHECK_EQ(bytesForRecord, 8) <<
      "Record size was not correct";
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
  uint64_t expected_bytes_for_each_record =
      ParquetColumn::BytesForDataType(parquet::Type::INT32);

  CheckRecordMetadata(output,
                      5,
                      { expected_bytes_for_each_record });
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
  uint64_t expected_bytes_for_each_record = 0;
  CheckRecordMetadata(output,
                      500,
                      { expected_bytes_for_each_record });
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
  vector<uint64_t> expected_bytes_for_each_record = { 0, 4 };
  CheckRecordMetadata(output,
                      1000,
                      expected_bytes_for_each_record);
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
  vector<uint64_t> expected_bytes_for_each_record = { 4 };
  CheckRecordMetadata(output,
                      500,
                      expected_bytes_for_each_record);
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
  vector<uint64_t> expected_bytes_for_each_record = { 4, 0 };
  CheckRecordMetadata(output,
                      500,
                      expected_bytes_for_each_record);
}

}  // namespace

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  google::InitGoogleLogging(argv[0]);
  return RUN_ALL_TESTS();
}
