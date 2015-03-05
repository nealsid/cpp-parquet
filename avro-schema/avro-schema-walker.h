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
  virtual bool AtNode(const NodePtr& node, const vector<string>& names,
                      int level, void* data_from_parent, void** data_for_children) = 0;
};

class AvroSchemaWalker {
 public:
  explicit AvroSchemaWalker(const string& json_file);
  void WalkSchema(AvroSchemaCallback* callback) const;
 private:
  void StartWalk(const NodePtr node,
                 vector<string>* name,
                 int level, AvroSchemaCallback* callback,
                 void* data_from_parent) const;
  avro::ValidSchema schema_;
};

class AvroSchemaToParquetSchemaConverter : public AvroSchemaCallback {
 public:
  AvroSchemaToParquetSchemaConverter();
  bool AtNode(const NodePtr& node,
              const vector<string>& names,
              int level,
              void* data_from_parent, void** data_for_children);

  ParquetColumn* Root();
 private:
  ParquetColumn* AvroNodePtrToParquetColumn(const NodePtr& node,
                                            const vector<string>& names,
                                            int level) const;
  ParquetColumn* root_;
};

}  // namespace parquet_file

#endif  // AVRO_SCHEMA_AVRO_SCHEMA_WALKER_H_
