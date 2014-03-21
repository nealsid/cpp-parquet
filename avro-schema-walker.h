// Copyright 2014 Mount Sinai School of Medicine

#include "avro/Node.hh"
#include "avro/ValidSchema.hh"

#include <string>

#ifndef __AVRO_SCHEMA_WALKER_H__
#define __AVRO_SCHEMA_WALKER_H__

using avro::NodePtr;
using avro::ValidSchema;
using std::string;

class AvroSchemaCallback {
 public:
  virtual void AtNode(const NodePtr& node, int level) const = 0;
} ;

class AvroSchemaWalker {
 public:
  AvroSchemaWalker(const string& json_file);
  void WalkSchema(const AvroSchemaCallback* callback) const;
  void StartWalk(const NodePtr node, int level, const AvroSchemaCallback* callback) const;
 private:
  avro::ValidSchema schema_;
};


#endif  // #ifdef __AVRO_SCHEMA_WALKER_H__
