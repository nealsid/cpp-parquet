// Copyright 2014 Mount Sinai School of Medicine

#include "parquet_types.h"
#include <glog/logging.h>
#include <string>
#include <vector>

#ifndef __PARQUET_COLUMN_H__
#define __PARQUET_COLUMN_H__

using parquet::ColumnChunk;
using parquet::ColumnMetaData;
using parquet::DataPageHeader;
using parquet::FieldRepetitionType;
using parquet::Type;
using std::string;
using std::to_string;
using std::vector;

namespace parquet_file {
// ParquetColumn is the base class for a Parquet Column.  A pointer of
// this type can point to either this class or ParquetDataColumn.
// ParquetColumn can contain children, which is how an, for example,
// Apache Avro message could be represented.  TODO: Figure out whether
// we really need a class hierarchy for this or, should we just
// represent both container and data columns as the same class with
// different fields filled in.
class ParquetColumn {
public:
  // Takes a name and the reptition type (defined by Parquet)
  ParquetColumn(const string& column_name,
		FieldRepetitionType::type repetition_type);
  // Set/get the children of this column
  void SetChildren(const vector<ParquetColumn*>& child);
  const vector<ParquetColumn*>& Children() const;

  // Accessors for the reptition type, name.
  FieldRepetitionType::type RepetitionType() const;
  string Name() const;
  // Pretty printing method.
  virtual string ToString() const;
 protected:
 private:
  // The name of the column.
  string column_name_;
  // Parquet type indicating whether the field is required, or
  // repeated, etc.
  FieldRepetitionType::type repetition_type_;
  // A list of columns that are children of this one.
  vector<ParquetColumn*> children_;
};

// A subclass of ParquetColumn that represents a parquet column that
// stores data, rather than grouping other columns.
class ParquetDataColumn : public ParquetColumn {
 public:
  // Constructor that takes a name, data type (see Parquet thrift file
  // for types), and repetition type.
  ParquetDataColumn(const string& name, Type::type data_type, 
		    FieldRepetitionType::type repetition_type);
  // Accessor and pretty-printing
  Type::type Type() const;
  virtual string ToString() const;
 private:
  // These represent the Parquet structures that will track this
  // column on-disk.
  vector<ColumnChunk> data_chunks_;
  ColumnMetaData column_metadata_;
  DataPageHeader data_header_;
  
  // Parquet datatype for this column.
  Type::type data_type_;
};

// A callback class for use with DepthFirstSchemaTraversal.  The
// callback is called for each column in a depth-first, preorder,
// traversal.
class ParquetColumnWalker {
public:
  void ColumnCallback(const ParquetColumn& column) {
    VLOG(2) << column.ToString();
  }
};

}  // namespace parquet_file

#endif  // #ifndef __PARQUET_COLUMN_H__
