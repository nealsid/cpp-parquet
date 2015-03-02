#ifndef PARQUET_FILE_AVRO_PARQUET_ENCODER_H_
#define PARQUET_FILE_AVRO_PARQUET_ENCODER_H_

#include <avro/Encoder.hh>

namespace parquet_file {

class AvroSchemaWalker;
class ParquetFile;

class AvroParquetEncoder : public avro::Encoder {
 public:
  AvroParquetEncoder(std::string json_schema_filename);
 private:
  std::unique_ptr<AvroSchemaWalker> avro_schema_walker_;
  std::unique_ptr<ParquetFile> parquet_file_;
 public:
  // I'm putting these in a separate public section because they're
  // just inherited methods from the avro::Encoder class.
  ~AvroParquetEncoder();
  /// All future encodings will go to os, which should be valid until
  /// it is reset with another call to init() or the encoder is
  /// destructed.
  void init(avro::OutputStream& os);

  /// Flushes any data in internal buffers.
  void flush();

  /// Encodes a null to the current stream.
  void encodeNull();

  /// Encodes a bool to the current stream
  void encodeBool(bool b);

  /// Encodes a 32-bit int to the current stream.
  void encodeInt(int32_t i);

  /// Encodes a 64-bit signed int to the current stream.
  void encodeLong(int64_t l);

  /// Encodes a single-precision floating point number to the current stream.
  void encodeFloat(float f);

  /// Encodes a double-precision floating point number to the current stream.
  void encodeDouble(double d);

  /// Encodes a UTF-8 string to the current stream.
  void encodeString(const std::string& s);

  /**
   * Encodes aribtray binary data into tthe current stream as Avro "bytes"
   * data type.
   * \param bytes Where the data is
   * \param len Number of bytes at \p bytes.
   */
  void encodeBytes(const uint8_t *bytes, size_t len);

  /**
   * Encodes aribtray binary data into tthe current stream as Avro "bytes"
   * data type.
   * \param bytes The data.
   */
  void encodeBytes(const std::vector<uint8_t>& bytes);

  /// Encodes fixed length binary to the current stream.
  void encodeFixed(const uint8_t *bytes, size_t len);

  /**
   * Encodes an Avro data type Fixed.
   * \param bytes The fixed, the length of which is taken as the size
   * of fixed.
   */
  void encodeFixed(const std::vector<uint8_t>& bytes);

  /// Encodes enum to the current stream.
  void encodeEnum(size_t e);

  /// Indicates that an array of items is being encoded.
  void arrayStart();

  /// Indicates that the current array of items have ended.
  void arrayEnd();

  /// Indicates that a map of items is being encoded.
  void mapStart();

  /// Indicates that the current map of items have ended.
  void mapEnd();

  /// Indicates that count number of items are to follow in the current array
  /// or map.
  void setItemCount(size_t count);

  /// Marks a beginning of an item in the current array or map.
  void startItem();

  /// Encodes a branch of a union. The actual value is to follow.
  void encodeUnionIndex(size_t e);
};

}  // namespace

#endif  // #ifdef PARQUET_FILE_AVRO_PARQUET_ENCODER_H_
