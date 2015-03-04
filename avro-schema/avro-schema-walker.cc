#include <avro-schema/avro-schema-walker.h>
#include <avro/Compiler.hh>
#include <avro/Node.hh>
#include <avro/Types.hh>
#include <avro/ValidSchema.hh>
#include <glog/logging.h>
#include <parquet-file/parquet-column.h>
#include <parquet-file/parquet-file.h>
#include <fstream>

using parquet_file::ParquetColumn;
using parquet_file::ParquetFile;

namespace parquet_file {

AvroSchemaWalker::AvroSchemaWalker(const string& json_file) {
  std::ifstream in(json_file.c_str());
  avro::compileJsonSchema(in, schema_);
}

void AvroSchemaWalker::WalkSchema(AvroSchemaCallback* callback) const {
  const NodePtr& root = schema_.root();
  vector<string> names;
  StartWalk(root, false, &names, 0, callback, nullptr);
}

void AvroSchemaWalker::StartWalk(const NodePtr node,
                                 bool union_with_null,
                                 vector<string>* names,
                                 int level,
                                 AvroSchemaCallback* callback,
                                 void* data_for_children) const {
  VLOG(2) << "Inside StartWalk. Level: " << level << ". Leaf count: "
          << node->leaves() << ". Name count: " << node->names() << ". Type: "
          << node->type();
  void* parent_data = callback->AtNode(node, *names, union_with_null, level, data_for_children);
  // Although the current node might a union with a null, it doesn't
  // mean our children are optional.  Put another way, this node can
  // be optional, but if this node is present defined, all subfields
  // may still be required, so by setting union_with_null back to
  // false, we're preventing a union_with_null from propagating down
  // the subtree of the schema.
  union_with_null = false;
  VLOG(2) << "logging names";
  for (int j = 0; j < node->names() ; ++j) {
    VLOG(2) << "\t" << node->nameAt(j);
  }
  for (int i = 0; i < node->leaves(); ++i) {

    bool pushed = false;
    if (node->names() > i) {
      string name_for_leaf = node->nameAt(i);
      names->push_back(name_for_leaf);
      VLOG(2) << "Name of leaf " << i << ": " << name_for_leaf;
      pushed = true;
    }

    // For some reason the leaf nodes in an Avro Schema tree are not
    // very useful - they only contain type information, but not the
    // name.  So we read the child name from the parent and use that
    // in the callback for the child.
    const NodePtr& current_leaf = node->leafAt(i);
    if (current_leaf->type() == avro::AVRO_UNION) {
      // Search for a "null" so that we can make this column optional in the Parquet schema.
      int null_leaf_index = -1;
      for (int j = 0; j < current_leaf->leaves(); ++j) {
        if (current_leaf->leafAt(j)->type() == avro::AVRO_NULL) {
          null_leaf_index = j;
          break;
        }
      }
      CHECK(null_leaf_index != -1 && current_leaf->leaves() == 2) <<
          "Parquet does not support unions of multiple non-null " <<
          " subtypes without defining multiple columns for each subtype of " <<
          " the union. (https://issues.apache.org/jira/browse/PARQUET-155)";
      // We do 1 - null_leaf_index to pick the opposite node of the one that's the
      // null node.
      VLOG(2) << "\twalking down non-null subtree of union node";
      StartWalk(current_leaf->leafAt(1 - null_leaf_index),
                true, names, level + 1, callback, parent_data);
    } else {
      StartWalk(node->leafAt(i), union_with_null, names, level + 1,
                callback, parent_data);
    }
    if (pushed) {
      names->pop_back();
    }
  }
}

AvroSchemaToParquetSchemaConverter::AvroSchemaToParquetSchemaConverter() :
  root_(nullptr) {
}

void* AvroSchemaToParquetSchemaConverter::AtNode(const NodePtr& node,
                                                 const vector<string>& names,
                                                 bool union_with_null,
                                                 int level,
                                                 void* parent_data) {
  // This is a very ugly hack.  Normally, Parquet requires fields to
  // have a dot-joined name of the schema path in the
  // metadata. I.e. "a.b.c".  This is accomplished by the "names"
  // parameter, which is a vector of strings that is appended to as
  // the depth-first-traversal of the schema happens.  As a special
  // case, this schema path name should NOT contain the outer message
  // name.  However, if we don't embed the outer message name as the
  // root column's name, tools like parquet-schema (or presumably,
  // anything that reads parquet), won't be able to know the name of
  // the message contained in the file, which isn't horrible...but it
  // is a regression compared to other tools.  So we do this special
  // hack to make sure the root column has the outer message name, but
  // also ensure that the outer message name is not part of the schema
  // path of any fields further down in the schema tree.
  ParquetColumn* column = nullptr;
  if (level == 0 && parent_data == nullptr && names.size() == 0) {
    LOG_IF(FATAL, node->type() != avro::AVRO_RECORD)
      << "Root node was not AVRO_RECORD";
    vector<string> outer_message_name = {node->name().fullname()};
    column = AvroNodePtrToParquetColumn(node, union_with_null,
                                        outer_message_name, level);
    LOG_IF(WARNING, root_ != nullptr) << "Root being overwritten";
    root_ = column;
    VLOG(3) << column->ToString();
    return column;
  }

  LOG_IF(FATAL, parent_data == nullptr)
    << "No parent data passed into callback for child node";
  column = AvroNodePtrToParquetColumn(node, union_with_null, names, level);

  ParquetColumn* parent = (ParquetColumn*) parent_data;
  parent->AddChild(column);

  VLOG(3) << column->ToString();
  return column;
}

ParquetColumn* AvroSchemaToParquetSchemaConverter::Root() {
  return root_;
}

ParquetColumn*
AvroSchemaToParquetSchemaConverter::AvroNodePtrToParquetColumn(
    const NodePtr& node,
    bool union_with_null,
    const vector<string>& names,
    int level) const {
  avro::Type avro_type = node->type();
  ParquetColumn* c = NULL;
  if (node->type() == avro::AVRO_ARRAY) {
    CHECK(node->leaves() == 1);
    CHECK(node->leafAt(0)->names() == 0);
    CHECK(node->leafAt(0)->type() == avro::AVRO_INT);
    c = new ParquetColumn(names, parquet::Type::INT32,
        1, 1,
			  FieldRepetitionType::REPEATED,
			  Encoding::PLAIN,
			  CompressionCodec::UNCOMPRESSED);
  } else {
    FieldRepetitionType::type repetition_type =
        union_with_null ? FieldRepetitionType::OPTIONAL : FieldRepetitionType::REQUIRED;

    c = new ParquetColumn(
        names, parquet::Type::INT32,
        1, 1,
        repetition_type,
        Encoding::PLAIN,
        CompressionCodec::UNCOMPRESSED);
  }

  return c;
}
}  // namespace parquet_file
