// Copyright 2014 Mount Sinai School of Medicine

#include <avro/Node.hh>
#include <avro/ValidSchema.hh>
#include <parquet-file/parquet-column.h>
#include "./parquet_types.h"

#include <map>
#include <string>
#include <vector>

#ifndef AVRO_SCHEMA_AVRO_SCHEMA_WALKER_H_
#define AVRO_SCHEMA_AVRO_SCHEMA_WALKER_H_

using avro::NodePtr;
using avro::ValidSchema;
using parquet_file::ParquetColumn;
using std::make_pair;
using std::string;
using std::vector;

namespace parquet_file {

static std::map<avro::Type, parquet::Type::type> type_mapping{
  // TODO (nealsid): remove this.
  make_pair(avro::AVRO_RECORD, parquet::Type::INT32),
  make_pair(avro::AVRO_INT, parquet::Type::INT32),
  make_pair(avro::AVRO_LONG, parquet::Type::INT64),
  // TODO (nealsid): and this.
  make_pair(avro::AVRO_BOOL, parquet::Type::BOOLEAN),
  make_pair(avro::AVRO_STRING, parquet::Type::BYTE_ARRAY),
  make_pair(avro::AVRO_BYTES, parquet::Type::BYTE_ARRAY),
  make_pair(avro::AVRO_FLOAT, parquet::Type::FLOAT),
  make_pair(avro::AVRO_DOUBLE, parquet::Type::DOUBLE),
      };

class AvroSchemaCallback {
 public:
  // Callback function for each
  virtual bool AtNode(const NodePtr& node,
                      bool optional,
                      bool array,
                      const vector<string>& names,
                      int level,
                      void* data_from_parent,
                      void** data_for_children) = 0;
};

class AvroSchemaWalker {
 public:
  explicit AvroSchemaWalker(const string& json_file);
  void WalkSchema(AvroSchemaCallback* callback);

private:
  bool LeafSubtreeRepresentsOptionalType(const NodePtr& node,
                                         int* child_of_leaf_index) const;

  void StartWalk(const NodePtr node,
                 bool optional,
                 bool array,
                 vector<string>* name,
                 int level, AvroSchemaCallback* callback,
                 void* data_from_parent);
  avro::ValidSchema schema_;
  std::map<string, const NodePtr> name_to_nodeptr_;
};

class AvroSchemaToParquetSchemaConverter : public AvroSchemaCallback {
 public:
  AvroSchemaToParquetSchemaConverter();
  bool AtNode(const NodePtr& node,
              bool optional,
              bool array,
              const vector<string>& names,
              int level,
              void* data_from_parent,
              void** data_for_children);

  ParquetColumn* Root();
 private:
  // Helper method to convert an AVRO NodePtr to a ParquetColumn.
  // Requires that the NodePtr is of a primitive AVRO type.
  ParquetColumn* AvroNodePtrToParquetColumn(const NodePtr& node,
                                            bool optional,
                                            bool array,
                                            const vector<string>& names,
                                            int level) const;
  ParquetColumn* root_;
};

}  // namespace parquet_file

#endif  // AVRO_SCHEMA_AVRO_SCHEMA_WALKER_H_
