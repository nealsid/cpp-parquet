#include "avro-schema-walker.h"
#include <avro/Compiler.hh>
#include <avro/Types.hh>
#include <avro/ValidSchema.hh>
#include <glog/logging.h>
#include "parquet-column.h"
#include "parquet-file.h"
#include <fstream>

using parquet_file::ParquetColumn;
using parquet_file::ParquetFile;

AvroSchemaWalker::AvroSchemaWalker(const string& json_file) {
  std::ifstream in(json_file.c_str());
  avro::compileJsonSchema(in, schema_);
}

void AvroSchemaWalker::WalkSchema(AvroSchemaCallback* callback) const {
  const NodePtr& root = schema_.root();
  vector<string> names;
  StartWalk(root, names, 0, callback, NULL);
}

void AvroSchemaWalker::StartWalk(const NodePtr node, vector<string>& names,
				 int level,
				 AvroSchemaCallback* callback,
				 void* data_for_children) const {
  void* parent_data = callback->AtNode(node, names, level, data_for_children);
  for (int i = 0; i < node->leaves(); ++i) {
    // For some reason the leaf nodes in an Avro Schema tree are not
    // very useful - they only contain type information, but not the
    // name.  So we read the child name from the parent and call use
    // that in the callback for the child.
    names.push_back(node->nameAt(i));
    StartWalk(node->leafAt(i), names, level + 1, callback,
	      parent_data);
    names.pop_back();
  }
}

class AvroSchemaToParquetSchemaConverter : public AvroSchemaCallback {
 public:
  AvroSchemaToParquetSchemaConverter() :
    root_(nullptr) {
  }

  void* AtNode(const NodePtr& node, vector<string>& names, int level,
	       void* parent_data) {
    ParquetColumn* column = AvroNodePtrToParquetColumn(node, names, level);

    if (parent_data != nullptr) {
      ParquetColumn* parent = (ParquetColumn*) parent_data;
      parent->AddChild(column);
    } else {
      LOG_IF(WARNING, root_ != nullptr) << "Root being overwritten";
      root_ = column;
    }

    VLOG(2) << column->ToString();
    return column;
  }

  ParquetColumn* Root() {
    return root_;
  }
 private:
  ParquetColumn* AvroNodePtrToParquetColumn(const NodePtr& node,
					    const vector<string>& name,
					    int level) const {
    avro::Type avro_type = node->type();
    // LOG_IF(FATAL, avro_type != avro::AVRO_INT)
    //   << "Non-integers are not supported today";
    ParquetColumn* c = new ParquetColumn(name, parquet::Type::INT32,
					 FieldRepetitionType::REQUIRED,
					 Encoding::PLAIN,
					 CompressionCodec::UNCOMPRESSED);

    return c;
  }
  ParquetColumn* root_;
};

int main(int argc, char* argv[]) {
  google::InitGoogleLogging(argv[0]);
  if (argc < 2) {
    LOG(FATAL) <<
      "Specify JSON schema file on command line";
    return 1;
  }
  AvroSchemaWalker walker(argv[1]);
  AvroSchemaToParquetSchemaConverter* converter = new AvroSchemaToParquetSchemaConverter();
  walker.WalkSchema(converter);
  ParquetColumn* root = converter->Root();
  ParquetFile* parquet_file = new ParquetFile("test.parquet");
  parquet_file->SetSchema(root);
  parquet_file->Flush();
}
