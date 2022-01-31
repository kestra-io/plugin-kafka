package io.kestra.plugin.kafka;

@io.swagger.v3.oas.annotations.media.Schema(
    title = "Serializer / Deserializer use for the value",
    description = "List are not handled."
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
