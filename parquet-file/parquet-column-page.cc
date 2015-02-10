#include "./parquet-column-page.h"

namespace parquet_file {

void ParquetColumnPage::AddRecords(void* buf, uint16_t repetition_level,
                                   uint32_t n) {
  CHECK_LT(repetition_level, max_repetition_level_) <<
      "For adding repeated data in this column, use AddRepeatedData";
  // TODO: check for overflow of multiply
  size_t num_bytes = n * bytes_per_datum_;
  memcpy(data_ptr_, buf, num_bytes);
  data_ptr_ += num_bytes;
  num_records_ += n;
  num_datums_ += n;
  for (int i = 0; i < n; ++i) {
    repetition_levels_.push_back(repetition_level);
    definition_levels_.push_back(max_definition_level_);
  }
}

// Adds repeated data to this column.  All data is considered part
// of the same record.
void ParquetColumnPage::AddRepeatedData(void *buf,
                                        uint16_t current_repetition_level,
                                        uint32_t n) {
  LOG_IF(FATAL, getFieldRepetitionType() != FieldRepetitionType::REPEATED) <<
      "Cannot add repeated data to a non-repeated column: " << FullSchemaPath();
  size_t num_bytes = n * bytes_per_datum_;
  memcpy(data_ptr_, buf, n * bytes_per_datum_);
  data_ptr_ += num_bytes;
  repetition_levels_.push_back(current_repetition_level);
  definition_levels_.push_back(max_definition_level_);
  for (int i = 1; i < n; ++i) {
    repetition_levels_.push_back(max_repetition_level_);
    definition_levels_.push_back(max_definition_level_);
  }
  num_records_ += 1;
  num_datums_ += n;
}

void ParquetColumnPage::AddNulls(uint16_t current_repetition_level,
                                 uint16_t current_definition_level,
                                 uint32_t n) {
  LOG_IF(FATAL, getFieldRepetitionType() != FieldRepetitionType::OPTIONAL) <<
      "Cannot add NULL to non-optional column: " << FullSchemaPath();
  for (int i = 0; i < n; ++i) {
    repetition_levels_.push_back(current_repetition_level);
    definition_levels_.push_back(current_definition_level);
  }
  num_records_ += n;
}

}  // namespace parquet_file
