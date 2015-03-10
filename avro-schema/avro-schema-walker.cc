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

void AvroSchemaWalker::WalkSchema(AvroSchemaCallback* callback) {
  const NodePtr& root = schema_.root();
  vector<string> names;
  StartWalk(root, false, false, &names, 0, callback, nullptr);
}

void AvroSchemaWalker::StartWalk(const NodePtr node,
                                 bool optional,
                                 bool array,
                                 vector<string>* names,
                                 int level,
                                 AvroSchemaCallback* callback,
                                 void* data_from_parent) {
  VLOG(2) << "Inside StartWalk. Level: " << level << ". Leaf count: "
          << node->leaves() << ". Name count: " << node->names() << ". Type: "
          << node->type();
  void* data_for_children = nullptr;
  bool continue_to_recurse = callback->AtNode(node, optional, array,
                                              *names, level,
                                              data_from_parent, &data_for_children);
  if (!continue_to_recurse) {
    return;
  }

  if (data_for_children == nullptr) {
    // The callback can set child data to NULL, in which case we
    // cascade the current data_from_parent to the next level of the
    // Avro tree.
    data_for_children = data_from_parent;
  }

  for (int i = 0; i < node->leaves(); ++i) {
    // For some reason the leaf nodes in an Avro Schema tree are not
    // very useful - they only contain type information, but not the
    // name.  So we read the child name from the parent and use that
    // in the callback for the child.
    size_t num_names = node->names();
    if (num_names > i) {
      const string& name_for_leaf = node->nameAt(i);
      names->push_back(name_for_leaf);
      VLOG(2) << "Name of leaf " << i << ": " << name_for_leaf;
    }
    int child_of_leaf_index = -1;
    NodePtr leaf = node->leafAt(i);
    NodePtr node_to_recurse_on(nullptr);
    if (LeafSubtreeRepresentsOptionalType(leaf,
                                          &child_of_leaf_index)) {
      optional = true;
      array = false;
      node_to_recurse_on = leaf->leafAt(child_of_leaf_index);
    } else if (leaf->type() == avro::AVRO_SYMBOLIC) {
      // TODO handle this by redirecting to map we've filled in.
    } else // if (LeafSubtreeRepresentsArrayType(node->leafAt(i), &child_of_leaf_index)) {
    //   array = true;
    //   optional = false;
    //   node_to_recurse_on = node->leafAt(child_of_leaf_index);
    // } else
    {
      optional = false;
      array = false;
      node_to_recurse_on = leaf;
    }
    CHECK(avro::isPrimitive(node_to_recurse_on->type()) ||
          node_to_recurse_on->type() == avro::AVRO_RECORD ||
          node_to_recurse_on->type() == avro::AVRO_SYMBOLIC)
        << "Node was not primitive or record: "
        << node_to_recurse_on->type() << "/" << node_to_recurse_on->name();

    if (node_to_recurse_on->type() == avro::AVRO_RECORD) {
      VLOG(2) << "Nested record: " << node->name();
      name_to_nodeptr_.insert(make_pair(node->name(), node));
    }

    StartWalk(node_to_recurse_on, optional, array,
              names, level + 1,
              callback, data_for_children);
    if (num_names > i) {
      names->pop_back();
    }
  }
}

bool AvroSchemaWalker::LeafSubtreeRepresentsOptionalType(const NodePtr& node,
                                                         int* child_of_leaf_index) const {
  if (node->type() != avro::AVRO_UNION) {
    return false;
  }

  // If we're processing a union node, we make sure it's the union of
  // 1 NULL and 1 non-null type
  // Search for a "null", becaues AVRO represents optional data as a
  // union of NULL and another type.
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
  *child_of_leaf_index = 1 - null_leaf_index;
  VLOG(2) << "Non-null leaf type: " << node->leafAt(*child_of_leaf_index)->type();
  return true;
}

AvroSchemaToParquetSchemaConverter::AvroSchemaToParquetSchemaConverter() :
  root_(nullptr) {
}

bool AvroSchemaToParquetSchemaConverter::AtNode(const NodePtr& node,
                                                bool optional,
                                                bool array,
                                                const vector<string>& names,
                                                int level,
                                                void* data_from_parent,
                                                void** data_for_children) {
  ParquetColumn* column = nullptr;
  CHECK_NOTNULL(data_for_children);
  // Special case the root, with some sanity checks and special behavior
  // for the column name (which represents the message name of the
  // outer message in the Parquet file)
  if (node->type() == avro::AVRO_RECORD &&
      level == 0 &&
      data_from_parent == nullptr &&
      names.size() == 0) {
    VLOG(3) << "Assigning root";
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
    vector<string> outer_message_name = {node->name().fullname()};
    column = AvroNodePtrToParquetColumn(node, optional, array,
                                        outer_message_name, level);
    LOG_IF(WARNING, root_ != nullptr) << "Root being overwritten";
    root_ = column;
    *data_for_children = column;
    VLOG(3) << column->ToString();
    return true;
  }

  LOG_IF(FATAL, data_from_parent == nullptr)
    << "No parent data passed into callback for child node";

  if (avro::isPrimitive(node->type()) || node->type() == avro::AVRO_RECORD) {
    column = AvroNodePtrToParquetColumn(node, optional, array, names, level);
    ((ParquetColumn*) data_from_parent)->AddChild(column);

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
    bool optional,
    bool array,
    const vector<string>& names,
    int level) const {
  avro::Type avro_type = node->type();
  CHECK(avro::isPrimitive(avro_type) || avro_type == avro::AVRO_RECORD)
      << "Non-primitive types not supported in this method: " << avro_type;
  ParquetColumn* c = nullptr;
  auto type_lookup = type_mapping.find(node->type());
  if (type_lookup == type_mapping.end()) {
    LOG(FATAL) << "Unsupported column data type: " << node->type();
  }
  parquet::Type::type column_data_type = (*type_lookup).second;
  c = new ParquetColumn(
      names, column_data_type,
      level, level,
      optional ? FieldRepetitionType::OPTIONAL : FieldRepetitionType::REQUIRED,
      Encoding::PLAIN,
      CompressionCodec::UNCOMPRESSED);

  return c;
}
}  // namespace parquet_file
