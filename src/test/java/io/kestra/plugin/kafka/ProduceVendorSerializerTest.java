package io.kestra.plugin.kafka;

import com.github.tomakehurst.wiremock.WireMockServer;
import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.plugin.kafka.registry.AwsGlueSchemaRegistry;
import io.kestra.plugin.kafka.serdes.MapToGenericRecordSerializer;
import io.kestra.plugin.kafka.serdes.SerdeType;
import jakarta.inject.Inject;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.serialization.Serializer;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.post;
import static com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo;
import static com.github.tomakehurst.wiremock.core.WireMockConfiguration.options;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@KestraTest
class ProduceVendorSerializerTest {
    private static final String AVRO_SCHEMA = """
        {
          "type": "record",
          "name": "twitter_schema",
          "namespace": "io.kestra.examples",
          "fields": [
            {"name": "username", "type": "string"},
            {"name": "tweet", "type": "string"}
          ]
        }
        """;

    @Inject
    private RunContextFactory runContextFactory;

    @SuppressWarnings("unchecked")
    private Serializer<Object> glueSerializer(WireMockServer glue) throws Exception {
        glue.stubFor(post(urlEqualTo("/"))
            .willReturn(aResponse()
                .withStatus(200)
                .withBody("""
                    {
                      "SchemaVersionId": "11111111-1111-1111-1111-111111111111",
                      "SchemaArn": "arn:aws:glue:eu-west-1:123456789012:schema/test-registry/twitter_schema",
                      "SchemaName": "twitter_schema",
                      "DataFormat": "AVRO",
                      "Status": "AVAILABLE",
                      "SchemaDefinition": "{\\"type\\":\\"record\\",\\"name\\":\\"twitter_schema\\",\\"namespace\\":\\"io.kestra.examples\\",\\"fields\\":[{\\"name\\":\\"username\\",\\"type\\":\\"string\\"},{\\"name\\":\\"tweet\\",\\"type\\":\\"string\\"}]}"
                    }
                    """)));

        AwsGlueSchemaRegistry registry = AwsGlueSchemaRegistry.builder()
            .type(AwsGlueSchemaRegistry.class.getName())
            .region(Property.ofValue("eu-west-1"))
            .endpoint(Property.ofValue("http://localhost:" + glue.port()))
            .accessKey(Property.ofValue("dummy"))
            .secretKey(Property.ofValue("dummy"))
            .build();

        RunContext runContext = runContextFactory.of();
        Serializer<Object> vendor = (Serializer<Object>) registry.getSerializer(runContext, SerdeType.AVRO);
        vendor.configure(Map.of(
            "dataFormat", "AVRO",
            "schemaName", "twitter_schema",
            "registry.name", "test-registry"
        ), false);
        return vendor;
    }

    @Test
    void vendorSerializerAcceptsTheMapOfAFlow() throws Exception {
        WireMockServer glue = new WireMockServer(options().dynamicPort());
        glue.start();
        try {
            Serializer<Object> vendor = glueSerializer(glue);
            // the value of a flow is a plain map, not an Avro object
            Map<String, Object> value = Map.of("username", "bob", "tweet", "hello");

            // this is the failure of the issue: the bare vendor serializer rejects a map
            assertThrows(RuntimeException.class, () -> vendor.serialize("topic", value));

            Serializer<Object> wrapped = Produce.vendorValueSerializer(vendor, SerdeType.AVRO, new AvroSchema(AVRO_SCHEMA));

            byte[] bytes = wrapped.serialize("topic", value);
            assertNotNull(bytes);
            // the Glue wire format starts with a header version byte of 3
            assertEquals(3, bytes[0]);
            assertTrue(bytes.length > 18, "expected the Glue header and the encoded record");

            // the producer calls this overload, so it must convert the map too
            byte[] withHeaders = wrapped.serialize("topic", new RecordHeaders(), value);
            assertNotNull(withHeaders);
            assertEquals(3, withHeaders[0]);
        } finally {
            glue.stop();
        }
    }

    @Test
    void onlyAvroWithASchemaIsWrapped() {
        Serializer<Object> vendor = new RecordingSerializer();

        assertSame(vendor, Produce.vendorValueSerializer(vendor, SerdeType.JSON, new AvroSchema(AVRO_SCHEMA)));
        assertSame(vendor, Produce.vendorValueSerializer(vendor, SerdeType.AVRO, null));
        assertInstanceOf(MapToGenericRecordSerializer.class, Produce.vendorValueSerializer(vendor, SerdeType.AVRO, new AvroSchema(AVRO_SCHEMA)));
    }

    @Test
    void headersAreForwardedToTheDelegate() {
        RecordingSerializer delegate = new RecordingSerializer();
        Serializer<Object> wrapped = new MapToGenericRecordSerializer(delegate, new AvroSchema(AVRO_SCHEMA).rawSchema());
        Headers headers = new RecordHeaders().add("trace", "abc".getBytes());

        wrapped.serialize("topic", headers, Map.of("username", "bob", "tweet", "hello"));

        assertEquals(1, delegate.withHeaders.size());
        assertEquals(0, delegate.withoutHeaders.size());
        assertSame(headers, delegate.seenHeaders);
        GenericRecord record = assertInstanceOf(GenericRecord.class, delegate.withHeaders.getFirst());
        assertEquals("bob", record.get("username"));
    }

    private static class RecordingSerializer implements Serializer<Object> {
        final List<Object> withoutHeaders = new ArrayList<>();
        final List<Object> withHeaders = new ArrayList<>();
        Headers seenHeaders;

        @Override
        public byte[] serialize(String topic, Object data) {
            withoutHeaders.add(data);
            return new byte[0];
        }

        @Override
        public byte[] serialize(String topic, Headers headers, Object data) {
            seenHeaders = headers;
            withHeaders.add(data);
            return new byte[0];
        }
    }
}
