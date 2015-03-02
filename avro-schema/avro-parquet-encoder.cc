#include "avro-parquet-encoder.h"
#include "avro-schema/avro-schema-walker.h"
#include "parquet-file/parquet-file.h"

using parquet_file::AvroSchemaWalker;
using parquet_file::AvroParquetEncoder;
using parquet_file::ParquetFile;

namespace parquet_file {

AvroParquetEncoder::AvroParquetEncoder(std::string json_schema_filename) {
  avro_schema_walker_.reset(new AvroSchemaWalker(json_schema_filename));
  std::unique_ptr<AvroSchemaToParquetSchemaConverter> converter(new AvroSchemaToParquetSchemaConverter());
  avro_schema_walker_->WalkSchema(converter.get());
  parquet_file_.reset(new ParquetFile("test.parquet"));
  parquet_file_->SetSchema(converter->Root());

}

AvroParquetEncoder::~AvroParquetEncoder() {
  parquet_file_->Flush();
}

void AvroParquetEncoder::init(avro::OutputStream& os) {

}

/// Flushes any data in internal buffers.
void AvroParquetEncoder::flush() {

}

/// Encodes a null to the current stream.
void AvroParquetEncoder::encodeNull() {

}

/// Encodes a bool to the current stream
void AvroParquetEncoder::encodeBool(bool b) {

}

/// Encodes a 32-bit int to the current stream.
void AvroParquetEncoder::encodeInt(int32_t i) {

}

/// Encodes a 64-bit signed int to the current stream.
void AvroParquetEncoder::encodeLong(int64_t l) {

}

/// Encodes a single-precision floating point number to the current stream.
void AvroParquetEncoder::encodeFloat(float f) {

}

/// Encodes a double-precision floating point number to the current stream.
void AvroParquetEncoder::encodeDouble(double d) {

}

/// Encodes a UTF-8 string to the current stream.
void AvroParquetEncoder::encodeString(const std::string& s) {

}

/**
 * Encodes aribtray binary data into tthe current stream as Avro "bytes"
 * data type.
 * \param bytes Where the data is
 * \param len Number of bytes at \p bytes.
 */
void AvroParquetEncoder::encodeBytes(const uint8_t *bytes, size_t len) {

}

/**
 * Encodes aribtray binary data into tthe current stream as Avro "bytes"
 * data type.
 * \param bytes The data.
 */
void AvroParquetEncoder::encodeBytes(const std::vector<uint8_t>& bytes) {
  uint8_t b;
  encodeBytes(bytes.empty() ? &b : &bytes[0], bytes.size());
}

// Encodes fixed length binary to the current stream.
void AvroParquetEncoder::encodeFixed(const uint8_t *bytes, size_t len) {

}

void AvroParquetEncoder::encodeFixed(const std::vector<uint8_t>& bytes) {
  encodeFixed(&bytes[0], bytes.size());
}

/// Encodes enum to the current stream.
void AvroParquetEncoder::encodeEnum(size_t e) {

}

/// Indicates that an array of items is being encoded.
void AvroParquetEncoder::arrayStart() {

}

/// Indicates that the current array of items have ended.
void AvroParquetEncoder::arrayEnd() {

}

/// Indicates that a map of items is being encoded.
void AvroParquetEncoder::mapStart() {

}

/// Indicates that the current map of items have ended.
void AvroParquetEncoder::mapEnd() {

}

/// Indicates that count number of items are to follow in the current array
/// or map.
void AvroParquetEncoder::setItemCount(size_t count) {

}

/// Marks a beginning of an item in the current array or map.
void AvroParquetEncoder::startItem() {

}

/// Encodes a branch of a union. The actual value is to follow.
void AvroParquetEncoder::encodeUnionIndex(size_t e) {

}

}  // namespace

int main(int argc, char* argv[]) {
  google::InitGoogleLogging(argv[0]);
  if (argc < 2) {
    LOG(FATAL) <<
      "Specify JSON schema file on command line";
    return 1;
  }
  AvroParquetEncoder encoder(argv[1]);
}
