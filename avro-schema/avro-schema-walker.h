// Copyright 2014 Mount Sinai School of Medicine

#include <avro/Node.hh>
#include <avro/ValidSchema.hh>
#include <parquet-file/parquet-column.h>

#include <string>

#ifndef __AVRO_SCHEMA_WALKER_H__
#define __AVRO_SCHEMA_WALKER_H__

using avro::NodePtr;
using avro::ValidSchema;
using parquet_file::ParquetColumn;
using std::string;
using std::vector;

namespace parquet_file {

class AvroSchemaCallback {
 public:
  virtual void* AtNode(const NodePtr& node, vector<string>& name, 
		      int level, void* parent_data) = 0;
} ;

class AvroSchemaWalker {
 public:
  AvroSchemaWalker(const string& json_file);
  void WalkSchema(AvroSchemaCallback* callback) const;
 private:
  void StartWalk(const NodePtr node, vector<string>& name,
		 int level, AvroSchemaCallback* callback,
		 void* parent_data) const;
  avro::ValidSchema schema_;
};

class AvroSchemaToParquetSchemaConverter : public AvroSchemaCallback {
 public:
  AvroSchemaToParquetSchemaConverter();
  void* AtNode(const NodePtr& node, vector<string>& names, int level,
	       void* parent_data);

  ParquetColumn* Root();
 private:
  ParquetColumn* AvroNodePtrToParquetColumn(const NodePtr& node,
					    const vector<string>& name,
					    int level) const;
  ParquetColumn* root_;
};

}  // namespace parquet_file

#endif  // #ifdef __AVRO_SCHEMA_WALKER_H__
