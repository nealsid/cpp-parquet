#include "avro-schema-walker.h"
#include <avro/ValidSchema.hh>
#include <avro/Compiler.hh>
#include <glog/logging.h>
#include <fstream>

AvroSchemaWalker::AvroSchemaWalker(const string& json_file) {
  std::ifstream in(json_file.c_str());
  avro::compileJsonSchema(in, schema_);
}

void AvroSchemaWalker::WalkSchema(const AvroSchemaCallback* callback) const {
  const NodePtr& root = schema_.root();
  StartWalk(root, 0, callback);
}

void AvroSchemaWalker::StartWalk(const NodePtr node, int level, const AvroSchemaCallback* callback) const {
  callback->AtNode(node, 0);
  for (int i = 0; i < node->leaves(); ++i) {
    StartWalk(node->leafAt(i), level + 1, callback);
  }
}

class DumbSchemaWalker : public AvroSchemaCallback {
  void AtNode(const NodePtr& node, int level) const {
    std::stringstream output_string;
    node->printBasicInfo(output_string);
    VLOG(2) << output_string.str();
  }
};

int main(int argc, char* argv[]) {
  google::InitGoogleLogging(argv[0]);
  if (argc < 2) {
    LOG(FATAL) <<
      "Specify JSON schema file on command line";
    return 1;
  }
  AvroSchemaWalker walker(argv[1]);
  walker.WalkSchema(new DumbSchemaWalker());
}
