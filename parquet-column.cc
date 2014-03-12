// Copyright 2014 Mount Sinai School of Medicine.

#include "parquet-column.h"
#include "thrift/protocol/TCompactProtocol.h"

using parquet::PageHeader;
using parquet::PageType;
using apache::thrift::protocol::TCompactProtocol;

namespace parquet_file {

ParquetColumn::ParquetColumn(const string& column_name,
			     parquet::Type::type data_type,
			     FieldRepetitionType::type repetition_type,
			     Encoding::type encoding,
			     CompressionCodec::type compression_codec)
  : column_name_(column_name),
    repetition_type_(repetition_type),
    encoding_(encoding),
    data_type_(data_type),
    num_rows_(0),
    compression_codec_(compression_codec),
    // I'm purposely using the constructor parameter in the next line,
    // as opposed to data_type_, in order to be clear that I'm am
    // avoiding a dependency on the order of variable declarations in
    // the class.
    bytes_per_datum_(BytesForDataType(data_type)),
    data_ptr_(data_buffer_),
    repetition_levels_(new uint8_t[kDataBufferSize / BytesForDataType(data_type)]),
    current_repetition_level_(0),
    definition_levels_(new uint8_t[kDataBufferSize / BytesForDataType(data_type)]),
    current_definition_level_(0),
    column_write_offset_(-1L) {
}

const vector<ParquetColumn*>& ParquetColumn::Children() const {
  return children_;
}

FieldRepetitionType::type ParquetColumn::RepetitionType() const {
  return repetition_type_;
}

Encoding::type ParquetColumn::Encoding() const {
  return encoding_;
}

Type::type ParquetColumn::Type() const {
  return data_type_;
}

CompressionCodec::type ParquetColumn::CompressionCodec() const {
  return compression_codec_;
}

string ParquetColumn::Name() const {
  return column_name_;
}

string ParquetColumn::ToString() const {
  // TODO: there has got to be an alternative to BOOST that supports
  // sane string concatenation and formatting.
  return this->Name() + "/" +
    parquet::_FieldRepetitionType_VALUES_TO_NAMES.at(this->RepetitionType())
    + "/" + to_string(Children().size()) + " children"
    + "/" + parquet::_Type_VALUES_TO_NAMES.at(this->Type())
    + "/" + to_string(num_rows_) + " rows"
    + "/" + to_string(bytes_per_datum_) + " bytes per datum";

}

void ParquetColumn::AddRows(void* buf, uint32_t n) {
  // TODO: check for overflow of multiply
  size_t num_bytes = n * bytes_per_datum_;
  memcpy(data_buffer_, buf, n * bytes_per_datum_);
  data_ptr_ += num_bytes;
  num_rows_ += n;
  for (int i = 0; i < n; ++i) {
    repetition_levels_.push_back(current_repetition_level_);
  }
}

// Adds repeated data to this column.  All data is considered part
// of the same record.
void ParquetColumn::AddRepeatedData(void *buf, uint32_t n) {
  LOG_IF(FATAL, RepetitionType() != FieldRepetitionType::REPEATED) <<
    "Cannot add repeated data to a non-repeated column: " << Name();
}

uint32_t ParquetColumn::NumRows() const {
  return num_rows_;
}

// static
uint8_t ParquetColumn::BytesForDataType(Type::type dataType) {
  // TODO support boolean (which is 1 bit)
  switch(dataType) {
  case Type::INT32:
  case Type::FLOAT:
    return 4;
  case Type::INT64:
  case Type::DOUBLE:
    return 8;
  case Type::INT96:
    return 12;
  case Type::BYTE_ARRAY:
  case Type::BOOLEAN:
  default:
    assert(0);
  }
}

void ParquetColumn::Flush(int fd, TCompactProtocol* protocol) {
  LOG_IF(FATAL, Encoding() != Encoding::PLAIN)
    << "Encoding can only be plain at this time.";
  LOG_IF(FATAL, CompressionCodec() != CompressionCodec::UNCOMPRESSED)
    << "Compression is not supported at this time.";

  column_write_offset_ = lseek(fd, 0, SEEK_CUR);

  PageHeader page_header;
  page_header.__set_type(PageType::DATA_PAGE);
  uint32_t total_data_bytes = BytesForDataType(data_type_) * NumRows();
  page_header.__set_uncompressed_page_size(total_data_bytes);
  // Obviously, this is a stop gap until compression support is added.
  page_header.__set_compressed_page_size(total_data_bytes);

  DataPageHeader data_header;
  data_header.__set_num_values(NumRows());
  data_header.__set_encoding(Encoding::PLAIN);
  // NB: For some reason, the following two must be set, even though
  // they can default to PLAIN, even for required/nonrepeating fields.
  // I'm not sure if it's part of the Parquet spec or a bug in
  // parquet-dump.
  data_header.__set_definition_level_encoding(Encoding::RLE);
  data_header.__set_repetition_level_encoding(Encoding::RLE);
  page_header.__set_data_page_header(data_header);
  uint32_t page_header_length = page_header.write(protocol);

  // We don't write repetition levels, because, for now, fields are
  // singular fields.
  // We don't write definition levels, because, for now, fields are
  // required.
  
  for (int i = 0; i < NumRows(); ++i) {
    LOG_IF(FATAL, data_buffer_ + (i * bytes_per_datum_) >= data_ptr_)
      << "Exceeded data added to internal buffer";
    ssize_t written = write(fd, data_buffer_ + i * bytes_per_datum_ , bytes_per_datum_);
    if (written != bytes_per_datum_) {
      LOG(FATAL) << "Did not write correct number of bytes for element %d\n", i;
    }
  }
}

ColumnMetaData ParquetColumn::ParquetColumnMetaData() const {
  ColumnMetaData column_metadata;
  uint32_t total_data_bytes = BytesForDataType(data_type_) * NumRows();
  column_metadata.__set_type(Type());
  column_metadata.__set_encodings({Encoding()});
  column_metadata.__set_path_in_schema({Name()});
  column_metadata.__set_codec(CompressionCodec());
  column_metadata.__set_num_values(NumRows());
  column_metadata.__set_total_uncompressed_size(total_data_bytes);
  column_metadata.__set_total_compressed_size(total_data_bytes);
  column_metadata.__set_data_page_offset(column_write_offset_);
  return column_metadata;
}
}  // namespace parquet_file
