package io.kestra.plugin.kafka.serdes;

@io.swagger.v3.oas.annotations.media.Schema(
    title = "Serializer / Deserializer to use for the value",
    description = "Lists are not supported."
)
public enum SerdeType {
    STRING,
    INTEGER,
    FLOAT,
    DOUBLE,
    LONG,
    SHORT,
    BYTE_ARRAY,
    BYTE_BUFFER,
    BYTES,
    UUID,
    VOID,
    AVRO,
    JSON
}
