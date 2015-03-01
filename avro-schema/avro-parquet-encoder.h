#include <avro/Encoder.hh>

class AvroParquetEncoder : public avro::Encoder {
 public:
  virtual ~AvroParquetEncoder() { };
  /// All future encodings will go to os, which should be valid until
  /// it is reset with another call to init() or the encoder is
  /// destructed.
  virtual void init(avro::OutputStream& os);

  /// Flushes any data in internal buffers.
  virtual void flush();

  /// Encodes a null to the current stream.
  virtual void encodeNull();

  /// Encodes a bool to the current stream
  virtual void encodeBool(bool b);

  /// Encodes a 32-bit int to the current stream.
  virtual void encodeInt(int32_t i);

  /// Encodes a 64-bit signed int to the current stream.
  virtual void encodeLong(int64_t l);

  /// Encodes a single-precision floating point number to the current stream.
  virtual void encodeFloat(float f);

  /// Encodes a double-precision floating point number to the current stream.
  virtual void encodeDouble(double d);

  /// Encodes a UTF-8 string to the current stream.
  virtual void encodeString(const std::string& s);

  /**
   * Encodes aribtray binary data into tthe current stream as Avro "bytes"
   * data type.
   * \param bytes Where the data is
   * \param len Number of bytes at \p bytes.
   */
  virtual void encodeBytes(const uint8_t *bytes, size_t len);

  /**
   * Encodes aribtray binary data into tthe current stream as Avro "bytes"
   * data type.
   * \param bytes The data.
   */
  void encodeBytes(const std::vector<uint8_t>& bytes) {
    uint8_t b;
    encodeBytes(bytes.empty() ? &b : &bytes[0], bytes.size());
  }

  /// Encodes fixed length binary to the current stream.
  virtual void encodeFixed(const uint8_t *bytes, size_t len);

  /**
   * Encodes an Avro data type Fixed.
   * \param bytes The fixed, the length of which is taken as the size
   * of fixed.
   */
  void encodeFixed(const std::vector<uint8_t>& bytes) {
    encodeFixed(&bytes[0], bytes.size());
  }

  /// Encodes enum to the current stream.
  virtual void encodeEnum(size_t e);

  /// Indicates that an array of items is being encoded.
  virtual void arrayStart();

  /// Indicates that the current array of items have ended.
  virtual void arrayEnd();

  /// Indicates that a map of items is being encoded.
  virtual void mapStart();

  /// Indicates that the current map of items have ended.
  virtual void mapEnd();

  /// Indicates that count number of items are to follow in the current array
  /// or map.
  virtual void setItemCount(size_t count);

  /// Marks a beginning of an item in the current array or map.
  virtual void startItem();

  /// Encodes a branch of a union. The actual value is to follow.
  virtual void encodeUnionIndex(size_t e);
};
