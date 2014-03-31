// Copyright 2014 Mount Sinai School of Medicine

#include "avro/Node.hh"
#include "avro/ValidSchema.hh"

#include <string>

#ifndef __AVRO_SCHEMA_WALKER_H__
#define __AVRO_SCHEMA_WALKER_H__

using avro::NodePtr;
using avro::ValidSchema;
using std::string;
using std::vector;

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


#endif  // #ifdef __AVRO_SCHEMA_WALKER_H__