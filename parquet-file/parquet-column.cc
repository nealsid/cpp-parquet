// Copyright 2014 Mount Sinai School of Medicine.

#include "./parquet-column.h"

#include <algorithm>
#include <bitset>
#include <boost/algorithm/string/join.hpp>
#include <boost/shared_array.hpp>
#include <parquet-file/util/rle-encoding.h>
#include <thrift/protocol/TCompactProtocol.h>

using apache::thrift::protocol::TCompactProtocol;
using parquet::PageHeader;
using parquet::PageType;

namespace parquet_file {

ParquetColumn::ParquetColumn(const vector<string>& column_name,
                             parquet::Type::type data_type,
                             uint16_t max_repetition_level,
                             uint16_t max_definition_level,
                             FieldRepetitionType::type repetition_type,
                             Encoding::type encoding,
                             CompressionCodec::type compression_codec,
                             boost::shared_array<uint8_t> data_buffer,
                             uint32_t data_buffer_size_in_bytes)
  : column_name_(column_name),
    repetition_type_(repetition_type),
    max_definition_level_(max_definition_level),
    max_repetition_level_(max_repetition_level),
    encoding_(encoding),
    data_type_(data_type),
    num_datums_(0),
    compression_codec_(compression_codec),
    // I'm purposely using the constructor parameter in the next line,
    // as opposed to data_type_, in order to be clear that I'm am
    // avoiding a dependency on the order of variable declarations in
    // the class.
    bytes_per_datum_(BytesForDataType(data_type)),
    column_write_offset_(-1L) {
  if (data_buffer.get() != nullptr) {
    data_buffer_.reset(data_buffer.get());
  } else {
    data_buffer_.reset(new uint8_t[1024000]);
  }
  data_ptr_ = data_buffer_.get();
}

ParquetColumn::ParquetColumn(const vector<string>& column_name,
                             FieldRepetitionType::type repetition_type)
  : column_name_(column_name),
    repetition_type_(repetition_type),
    num_datums_(0),
    data_type_(parquet::Type::BOOLEAN),
    column_write_offset_(-1L) {
}

const vector<ParquetColumn*>& ParquetColumn::Children() const {
  return children_;
}

FieldRepetitionType::type ParquetColumn::getFieldRepetitionType() const {
  return repetition_type_;
}

void ParquetColumn::setFieldRepetitionType(FieldRepetitionType::type repetition_type) {
  if (NumRecords() > 0) {
    LOG(WARNING) << "Changing column type after records added; are you sure?";
  }
  repetition_type_ = repetition_type;
}

Encoding::type ParquetColumn::getEncoding() const {
  return encoding_;
}

void ParquetColumn::setType(Type::type type) {
  if (NumRecords() > 0) {
    LOG(WARNING) << "Changing column type after records added; are you sure?";
  }
  data_type_ = type;
}

Type::type ParquetColumn::getType() const {
  return data_type_;
}

CompressionCodec::type ParquetColumn::getCompressionCodec() const {
  return compression_codec_;
}

string ParquetColumn::FullSchemaPath() const {
  if (column_name_.size() > 0) {
    return boost::algorithm::join(column_name_, ".");
  } else {
    return "";
  }
}

string ParquetColumn::Name() const {
  if (column_name_.size() > 0) {
    return column_name_.back();
  } else {
    return "";
  }
}

string ParquetColumn::ToString() const {
  return this->FullSchemaPath() + "/"
      + parquet::_FieldRepetitionType_VALUES_TO_NAMES.at(this->getFieldRepetitionType())
      + "/" + to_string(Children().size()) + " children"
      + "/" + parquet::_Type_VALUES_TO_NAMES.at(this->getType())
      + "/" + to_string(record_metadata.size()) + " records"
      + "/" + to_string(num_datums_) + " pieces of data"
      + "/" + to_string(bytes_per_datum_) + " bytes per datum";
}

void ParquetColumn::AddSingletonValueAsNRecords(void* buf,
                                                uint16_t repetition_level,
                                                uint32_t n) {
  CHECK_LT(repetition_level, max_repetition_level_) <<
    "For adding repeated data in this column, use AddRepeatedData";
  record_metadata.reserve(record_metadata.size() + n);
  repetition_levels_.reserve(repetition_levels_.size() + n);
  definition_levels_.reserve(definition_levels_.size() + n);
  // TODO: check for overflow of multiply
  num_datums_ += n;
  size_t rep_start = repetition_levels_.size();
  size_t def_start = definition_levels_.size();
  
  switch(bytes_per_datum_) {
    case 4:
      std::fill((uint32_t*)data_ptr_, (uint32_t*)(data_ptr_ + (bytes_per_datum_ * n)), *(uint32_t*)buf);
      break;
    case 8:
      std::fill((uint64_t*)data_ptr_, (uint64_t*)(data_ptr_ + (bytes_per_datum_ * n)), *(uint64_t*)buf);
      break;
    default:
      CHECK(0) << "Singleton fill for unsupported byte width";
  }
  repetition_levels_.insert(repetition_levels_.end(), n, repetition_level);
  definition_levels_.insert(definition_levels_.end(), n, max_definition_level_);
  for (int i = 0; i < n; ++i) {
    AddRecordMetadata(rep_start + i, rep_start + i + 1,
                      def_start + i, def_start + i + 1,
                      data_ptr_, data_ptr_ + bytes_per_datum_);
    data_ptr_ += bytes_per_datum_;
  }
}

void ParquetColumn::AddRecordMetadata(size_t rep_level_start, size_t rep_level_end,
                                      size_t def_level_start, size_t def_level_end,
                                      uint8_t* start, uint8_t* end) {
  RecordMetadata r;
  r.repetition_level_index_start = rep_level_start;
  r.repetition_level_index_end = rep_level_end;
  r.definition_level_index_start = def_level_start;
  r.definition_level_index_end = def_level_end;
  r.byte_begin = start;
  r.byte_end = end;
  record_metadata.push_back(r);
}

void ParquetColumn::AddRecords(void* buf, uint16_t repetition_level,
                               uint32_t n) {
  CHECK_LT(repetition_level, max_repetition_level_) <<
    "For adding repeated data in this column, use AddRepeatedData";
  record_metadata.reserve(record_metadata.size() + n);
  // TODO: check for overflow of multiply
  size_t num_bytes = n * bytes_per_datum_;
  memcpy(data_ptr_, buf, num_bytes);
  num_datums_ += n;
  size_t rep_start = repetition_levels_.size();
  size_t def_start = definition_levels_.size();

  for (int i = 0; i < n; ++i) {
    repetition_levels_.push_back(repetition_level);
    definition_levels_.push_back(max_definition_level_);
    AddRecordMetadata(rep_start + i, rep_start + i + 1,
                      def_start + i, def_start + i + 1,
                      data_ptr_, data_ptr_ + bytes_per_datum_);
  }
  data_ptr_ += num_bytes;
}

// Adds repeated data to this column.  All data is considered part
// of the same record.
void ParquetColumn::AddRepeatedData(void *buf,
                                    uint16_t current_repetition_level,
                                    uint32_t n) {
  LOG_IF(FATAL, getFieldRepetitionType() != FieldRepetitionType::REPEATED) <<
    "Cannot add repeated data to a non-repeated column: " << FullSchemaPath();
  size_t num_bytes = n * bytes_per_datum_;
  memcpy(data_ptr_, buf, n * bytes_per_datum_);

  size_t rep_start = repetition_levels_.size();
  size_t def_start = definition_levels_.size();

  repetition_levels_.push_back(current_repetition_level);
  repetition_levels_.insert(repetition_levels_.end(), n - 1, max_repetition_level_);

  definition_levels_.insert(definition_levels_.end(), n, max_definition_level_);

  AddRecordMetadata(rep_start, rep_start + n,
                    def_start, def_start + n,
                    data_ptr_, data_ptr_ + num_bytes);

  data_ptr_ += num_bytes;
  num_datums_ += n;
}

void ParquetColumn::AddNulls(uint16_t current_repetition_level,
                             uint16_t current_definition_level,
                             uint32_t n) {
  LOG_IF(FATAL, getFieldRepetitionType() != FieldRepetitionType::OPTIONAL) <<
    "Cannot add NULL to non-optional column: " << FullSchemaPath();
  record_metadata.reserve(record_metadata.size() + n);
  size_t rep_start = repetition_levels_.size();
  size_t def_start = definition_levels_.size();
  for (int i = 0; i < n; ++i) {
    repetition_levels_.push_back(current_repetition_level);
    definition_levels_.push_back(current_definition_level);

    AddRecordMetadata(rep_start + i, rep_start + i + 1,
                      def_start + i, def_start + i + 1,
                      data_ptr_, data_ptr_);
  }
}

// Adds binary data to this column as a single record.
void ParquetColumn::AddVariableLengthByteArray(
    void* buf,
    uint16_t current_repetition_level,
    uint32_t length) {
  LOG_IF(FATAL, getType() != parquet::Type::BYTE_ARRAY) <<
      "Column is not of type BYTE_ARRAY";
  CHECK_GT(length, 0) << "Use AddNulls to add a null element";
  size_t rep_start = repetition_levels_.size();
  size_t def_start = definition_levels_.size();

  repetition_levels_.push_back(current_repetition_level);
  definition_levels_.push_back(max_definition_level_);

  AddRecordMetadata(rep_start, rep_start + 1,
                    def_start, def_start + 1,
                    data_ptr_, data_ptr_ + 4 + length);
  memcpy(data_ptr_, &length, 4);
  data_ptr_ += 4;
  memcpy(data_ptr_, buf, length);
  data_ptr_ += length;
}

uint32_t ParquetColumn::NumRecords() const {
  return record_metadata.size();
}

uint32_t ParquetColumn::NumDatums() const {
  return num_datums_;
}

// static
uint8_t ParquetColumn::BytesForDataType(Type::type dataType) {
  // TODO support boolean (which is 1 bit)
  switch (dataType) {
  case Type::INT32:
  case Type::FLOAT:
    return 4;
  case Type::INT64:
  case Type::DOUBLE:
    return 8;
  case Type::INT96:
    return 12;
  case Type::BYTE_ARRAY:
    return 0;
  case Type::BOOLEAN:
  // TODO(nealsid): fix this
    return 1;
  default:
    assert(0);
  }
}

void ParquetColumn::SetChildren(const vector<ParquetColumn*>& children) {
  if (children_.size() > 0) {
    LOG(WARNING) << "Clearing pre-existing children in column: " << ToString();
    // NB The memory ownership semantics of children column pointers
    // needs to be worked out, but I know in this code path there is a
    // memory leak so I will just call delete here.
    for (auto c : children_) {
      delete c;
    }
  }
  children_.assign(children.begin(), children.end());
}

void ParquetColumn::AddChild(ParquetColumn* child) {
  children_.push_back(child);
}

void ParquetColumn::EncodeLevels(const vector<uint8_t>& level_vector,
                                 vector<uint8_t>* output_vector,
                                 uint16_t max_level) {
  CHECK_NOTNULL(output_vector);
  int max_buffer_size =
      impala::RleEncoder::MaxBufferSize(level_vector.size(),
                                        max_level);
  boost::shared_array<uint8_t> output_buffer(new uint8_t[max_buffer_size]);
  impala::RleEncoder encoder(output_buffer.get(), max_buffer_size, max_level);
  VLOG(2) << "\tLevels size: " << level_vector.size();
  for (uint8_t level : level_vector) {
    VLOG(3) << "\t\t" << to_string(level);
    CHECK(encoder.Put(level));
  }
  encoder.Flush();
  uint32_t num_bytes = encoder.len();
  VLOG(2) << "\tLevels occupy " << num_bytes
          << " bytes encoded";
  output_vector->assign(output_buffer.get(), output_buffer.get() + num_bytes);
  VLOG(2) << "\tOutput vector size: " << output_vector->size();
  if (VLOG_IS_ON(2)) {
    int bytes_to_log = 6;
    VLOG(2) << "\tLevel bitstream (first "
            << bytes_to_log << " bytes only): ";
    for (int i = 0; i < output_vector->size() && i < bytes_to_log; ++i) {
      VLOG(2) << "\t" << std::bitset<8>(output_vector->at(i));
    }
  }
}

void ParquetColumn::EncodeRepetitionLevels(
    vector<uint8_t>* encoded_repetition_levels) {
  CHECK_NOTNULL(encoded_repetition_levels);
  encoded_repetition_levels->clear();
  if (getFieldRepetitionType() == FieldRepetitionType::REPEATED) {
    VLOG(2) << "\tRepeated field, encoding repetition levels";
    EncodeLevels(repetition_levels_,
                 encoded_repetition_levels,
                 max_repetition_level_);
  } else {
    VLOG(2) << "\tNon-repeated field, skipping repetition levels";
  }
}

void ParquetColumn::EncodeDefinitionLevels(
    vector<uint8_t>* encoded_definition_levels) {
  CHECK_NOTNULL(encoded_definition_levels);
  encoded_definition_levels->clear();
  FieldRepetitionType::type repetition_type = getFieldRepetitionType();
  if (repetition_type == FieldRepetitionType::REPEATED ||
      repetition_type == FieldRepetitionType::OPTIONAL) {
    VLOG(2) << "\tRepeated or optional field, encoding definition levels";
    EncodeLevels(definition_levels_,
                 encoded_definition_levels,
                 max_definition_level_);
  } else {
    VLOG(2) << "\tSingular required field, skipping definition levels";
  }
}

size_t ParquetColumn::ColumnDataSizeInBytes() {
  if (Children().size() != 0) {
    return 0;
  }

  if (data_type_ != Type::BYTE_ARRAY) {
    return bytes_per_datum_ * num_datums_;
  }

  size_t record_size_accum = 0;
  for (auto r : record_metadata) {
    record_size_accum += (r.byte_end - r.byte_begin);
  }
  return record_size_accum;
}

void ParquetColumn::Flush(int fd,
                          TCompactProtocol* protocol) {
  LOG_IF(FATAL, getEncoding() != Encoding::PLAIN)
    << "Encoding can only be plain at this time.";
  LOG_IF(FATAL, getCompressionCodec() != CompressionCodec::UNCOMPRESSED)
    << "Compression is not supported at this time.";
  LOG_IF(FATAL, Children().size() != 0)  <<
      "Flush called on container column";

  column_write_offset_ = lseek(fd, 0, SEEK_CUR);
  VLOG(2) << "Inside flush for " << FullSchemaPath();
  size_t column_data_size = ColumnDataSizeInBytes();
  VLOG(2) << "\tData size: " << column_data_size << " bytes.";
  VLOG(2) << "\tNumber of records for this flush: " <<  NumRecords();
  VLOG(2) << "\tFile offset: " << column_write_offset_;
  vector<uint8_t> encoded_repetition_levels, encoded_definition_levels;
  EncodeRepetitionLevels(&encoded_repetition_levels);
  EncodeDefinitionLevels(&encoded_definition_levels);
  uint32_t repetition_level_size = encoded_repetition_levels.size();
  uint32_t definition_level_size = encoded_definition_levels.size();

  uncompressed_bytes_ = column_data_size + repetition_level_size +
                        definition_level_size;
  // We add 8 to this for the two ints at that indicate the length of
  // the rep & def levels.
  if (repetition_level_size > 0) {
    uncompressed_bytes_ += 4;
  }
  if (definition_level_size > 0) {
    uncompressed_bytes_ += 4;
  }

  DataPageHeader data_header;
  PageHeader page_header;
  page_header.__set_type(PageType::DATA_PAGE);

  page_header.__set_uncompressed_page_size(uncompressed_bytes_);
  // Obviously, this is a stop gap until compression support is added.
  page_header.__set_compressed_page_size(uncompressed_bytes_);
  data_header.__set_num_values(definition_levels_.size());
  data_header.__set_encoding(Encoding::PLAIN);
  // NB: For some reason, the following two must be set, even though
  // they can default to PLAIN, even for required/nonrepeating fields.
  // I'm not sure if it's part of the Parquet spec or a bug in
  // parquet-dump.
  data_header.__set_definition_level_encoding(Encoding::RLE);
  data_header.__set_repetition_level_encoding(Encoding::RLE);
  page_header.__set_data_page_header(data_header);
  uint32_t page_header_size = page_header.write(protocol);
  uncompressed_bytes_ += page_header_size;
  VLOG(2) << "\tPage header size: " << page_header_size;
  VLOG(2) << "\tTotal uncompressed bytes: " << uncompressed_bytes_;

  if (repetition_level_size > 0) {
    FlushLevels(fd, encoded_repetition_levels);
  }

  if (definition_level_size > 0) {
    FlushLevels(fd, encoded_definition_levels);
  }

  VLOG(2) << "\tData size: " << column_data_size;
  ssize_t written = write(fd, data_buffer_.get(),
                          column_data_size);
  if (written != column_data_size) {
    if (written == -1) {
      LOG(ERROR) << strerror(errno);
    }
    LOG(FATAL) << "Did not write correct number of bytes: " << written << "/" << column_data_size;
  }
  VLOG(2) << "\tData bytes written: " << written;
  VLOG(2) << "\tFinal offset after write: " << lseek(fd, 0, SEEK_CUR);
}

void ParquetColumn::FlushLevels(int fd, const vector<uint8_t>& levels_vector) {
  VLOG(3) << "\tOffset before writing size: " << lseek(fd, 0, SEEK_CUR);
  uint32_t num_elements = levels_vector.size();
  write(fd, &num_elements, 4);
  VLOG(3) << "\tOffset after writing size: " << lseek(fd, 0, SEEK_CUR);
  size_t bytes_written = write(fd, levels_vector.data(), num_elements);

  if (bytes_written != num_elements) {
    LOG(WARNING) << "Only " << bytes_written << " of "
                 << num_elements
                 << " for repetition levels were written.";
  }
  VLOG(3) << "\tOffset after levels written: " << lseek(fd, 0, SEEK_CUR);
}

ColumnMetaData ParquetColumn::ParquetColumnMetaData() const {
  ColumnMetaData column_metadata;
  column_metadata.__set_type(getType());
  column_metadata.__set_encodings({getEncoding()});
  column_metadata.__set_codec(getCompressionCodec());
  column_metadata.__set_num_values(definition_levels_.size());
  column_metadata.__set_total_uncompressed_size(uncompressed_bytes_);
  column_metadata.__set_total_compressed_size(uncompressed_bytes_);
  column_metadata.__set_data_page_offset(column_write_offset_);
  column_metadata.__set_path_in_schema(column_name_);
  return column_metadata;
}
}  // namespace parquet_file
