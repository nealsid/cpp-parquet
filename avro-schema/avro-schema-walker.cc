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
  StartWalk(root, &names, 0, callback, nullptr);
}

void AvroSchemaWalker::StartWalk(const NodePtr node,
                                 vector<string>* names,
                                 int level,
                                 AvroSchemaCallback* callback,
                                 void* data_from_parent) const {
  VLOG(2) << "Inside StartWalk. Level: " << level << ". Leaf count: "
          << node->leaves() << ". Name count: " << node->names() << ". Type: "
          << node->type();
  void* data_for_children;
  bool continue_to_recurse = callback->AtNode(node, *names, level,
                                              data_from_parent, &data_for_children);
  if (!continue_to_recurse) {
    return;
  }

  if (data_for_children == nullptr) {
    // The callback can return NULL, in which case we cascade the
    // current data_from_parent to the next level of the Avro tree.
    data_for_children = data_from_parent;
  }

  for (int i = 0; i < node->leaves(); ++i) {
    // For some reason the leaf nodes in an Avro Schema tree are not
    // very useful - they only contain type information, but not the
    // name.  So we read the child name from the parent and use that
    // in the callback for the child.
    bool pushed = false;
    if (node->names() > i) {
      const string& name_for_leaf = node->nameAt(i);
      names->push_back(name_for_leaf);
      VLOG(2) << "Name of leaf " << i << ": " << name_for_leaf;
      pushed = true;
    }
    StartWalk(node->leafAt(i), names, level + 1,
              callback, data_for_children);
    if (pushed) {
      names->pop_back();
    }
  }
}

AvroSchemaToParquetSchemaConverter::AvroSchemaToParquetSchemaConverter() :
  root_(nullptr) {
}

bool AvroSchemaToParquetSchemaConverter::AtNode(const NodePtr& node,
                                                const vector<string>& names,
                                                int level,
                                                void* data_from_parent,
                                                void** data_for_children) {
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
  CHECK_NOTNULL(data_for_children);
  if (level == 0 && data_from_parent == nullptr && names.size() == 0) {
    LOG_IF(FATAL, node->type() != avro::AVRO_RECORD)
      << "Root node was not AVRO_RECORD";
    vector<string> outer_message_name = {node->name().fullname()};
    column = AvroNodePtrToParquetColumn(node, outer_message_name, level);
    LOG_IF(WARNING, root_ != nullptr) << "Root being overwritten";
    root_ = column;
    *data_for_children = column;
    VLOG(3) << column->ToString();
    return true;
  }

  // If we're processing a union node, we do the following:
  // 1) Make sure it's the union of 1 NULL and 1 non-null type, and
  // recurse down the non-null node.
  // 2) We return false so that the schema walker doesn't recurse down
  // again.
  // 3) We call ourselves directly on the non-null child.

  if (node->type() == avro::AVRO_UNION) {
    // Search for a "null" so that we can make this column optional in the Parquet schema.
    int null_leaf_index = -1;
    for (int j = 0; j < node->leaves(); ++j) {
      if (node->leafAt(j)->type() == avro::AVRO_NULL) {
        // This is probably waaaaay overly-defensive, but, hey, never
        // trust user input.
        CHECK_EQ(null_leaf_index, -1) << "AVRO schema has union of two NULLs";
        null_leaf_index = j;
      }
    }
    CHECK(null_leaf_index != -1 && node->leaves() == 2) <<
        "Parquet does not support unions of multiple non-null " <<
        " subtypes without defining multiple columns for each subtype of " <<
        " the union. (https://issues.apache.org/jira/browse/PARQUET-155)";
    // We do 1 - null_leaf_index to pick the opposite node of the one that's the
    // null node.
    VLOG(2) << "\twalking down non-null subtree of union node";
    const NodePtr non_null_leaf = node->leafAt(1 - null_leaf_index);

    if (non_null_leaf->type() == avro::AVRO_SYMBOLIC) {
      return false;
    }
    CHECK(avro::isPrimitive(non_null_leaf->type()) ||
          non_null_leaf->type() == avro::AVRO_RECORD) <<
        "Parquet does not support optional arrays or other non-primitive "
        "types (the equivalent is an array with 0 elements) (type: " << non_null_leaf->type() << ")";

    AtNode(non_null_leaf, names, level + 1,
           data_from_parent, data_for_children);
    ((ParquetColumn*) (*data_for_children))->setFieldRepetitionType(
        FieldRepetitionType::OPTIONAL);
    return false;
  }

  LOG_IF(FATAL, data_from_parent == nullptr)
    << "No parent data passed into callback for child node";
  if (avro::isPrimitive(node->type()) || node->type() == avro::AVRO_RECORD) {
    column = AvroNodePtrToParquetColumn(node, names, level);

    ParquetColumn* parent = (ParquetColumn*) data_from_parent;
    parent->AddChild(column);

    VLOG(3) << column->ToString();
    *data_for_children = column;
    return true;
  }
  CHECK(false) << "Unsupported case";
}

ParquetColumn* AvroSchemaToParquetSchemaConverter::Root() {
  return root_;
}

ParquetColumn*
AvroSchemaToParquetSchemaConverter::AvroNodePtrToParquetColumn(
    const NodePtr& node,
    const vector<string>& names,
    int level) const {
  avro::Type avro_type = node->type();
  ParquetColumn* c = nullptr;
  if (node->type() == avro::AVRO_ARRAY) {
    CHECK(node->leaves() == 1);
    CHECK(node->leafAt(0)->names() == 0);
    CHECK(node->leafAt(0)->type() == avro::AVRO_INT);
    c = new ParquetColumn(
        names, parquet::Type::INT32,
        level, level,
			  FieldRepetitionType::REPEATED,
			  Encoding::PLAIN,
			  CompressionCodec::UNCOMPRESSED);
  } else {
    auto type_lookup = type_mapping.find(node->type());
    if (type_lookup == type_mapping.end()) {
      LOG(FATAL) << "Unsupported column data type: " << node->type();
    }
    parquet::Type::type column_data_type = (*type_lookup).second;
    c = new ParquetColumn(
        names, column_data_type,
        level, level,
        FieldRepetitionType::REQUIRED,
        Encoding::PLAIN,
        CompressionCodec::UNCOMPRESSED);
  }

  return c;
}
}  // namespace parquet_file
