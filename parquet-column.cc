// Copyright 2014 Mount Sinai School of Medicine.

#include "parquet-column.h"

namespace parquet_file {

ParquetColumn::ParquetColumn(const string& column_name, 
			     FieldRepetitionType::type repetition_type)
  : column_name_(column_name), 
    repetition_type_(repetition_type) {
}

const vector<ParquetColumn*>& ParquetColumn::Children() const { 
  return children_; 
}

FieldRepetitionType::type ParquetColumn::RepetitionType() const {
  return repetition_type_; 
}

string ParquetColumn::Name() const { 
  return column_name_; 
}

string ParquetColumn::ToString() const {
  // TODO: there has got to be an alternative to BOOST that supports
  // sane string concatenation and formatting.
  return this->Name() + "/" + 
    parquet::_FieldRepetitionType_VALUES_TO_NAMES.at(this->RepetitionType()) + 
    "/" + to_string(Children().size()) + " children";
}

ParquetDataColumn::ParquetDataColumn(const string& name, 
				     parquet::Type::type data_type,
				     FieldRepetitionType::type repetition_type)
  : ParquetColumn(name, repetition_type),
    num_rows_(0), 
    data_type_(data_type),
    // I'm purposely using the constructor parameter in the next line
    // due to the order of initialization not being defined.
    bytes_per_datum_(BytesForDataType(data_type)),
    data_ptr_(data_buffer_) {
}

Type::type ParquetDataColumn::Type() const { 
  return data_type_; 
}

string ParquetDataColumn::ToString() const {
  return ParquetColumn::ToString() 
    + "/" + parquet::_Type_VALUES_TO_NAMES.at(this->Type())
    + "/" + to_string(num_rows_) + " rows"
    + "/" + to_string(bytes_per_datum_) + " bytes per datum";
}

void ParquetDataColumn::AddRow(void* buf) {

}

// static
size_t ParquetDataColumn::BytesForDataType(Type::type dataType) {
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


}  // namespace parquet_file
