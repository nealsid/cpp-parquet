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
    "/" + to_string(Children().size());
}

ParquetDataColumn::ParquetDataColumn(const string& name, 
				     parquet::Type::type data_type,
				     FieldRepetitionType::type repetition_type)
  : ParquetColumn(name, repetition_type) {
  data_type_ = data_type;
}

Type::type ParquetDataColumn::Type() const { 
  return data_type_; 

}

string ParquetDataColumn::ToString() const {
  return ParquetColumn::ToString() 
    + "/" + parquet::_Type_VALUES_TO_NAMES.at(this->Type());
}

}  // namespace parquet_file
