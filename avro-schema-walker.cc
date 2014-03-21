#include "avro-schema-walker.h"
#include <avro/ValidSchema.hh>
#include <avro/Compiler.hh>
#include <glog/logging.h>

AvroSchemaWalker::AvroSchemaWalker(const string& json_file) {
  std::ifstream in(json_file);
  
  avro::compileJsonSchema(in, &schema_);
}

AvroSchemaWalker::WalkSchema(const AvroSchemaCallback* callback) {
  const NodePtr& root = schema_.root();
}

AvroSchemaWalker::StartWalk(const NodePtr& node, int level, const AvroSchemaCallback* callback) {
  callback->AtNode(node, 0, node.name().fullname(), callback);
  for (int i = 0; i < node.leaves(); ++i) {
    StartWalk(node.leafAt(i), level + 1, callback);
  }
}
