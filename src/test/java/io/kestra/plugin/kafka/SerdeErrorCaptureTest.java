package io.kestra.plugin.kafka;

import io.kestra.plugin.kafka.serdes.SerdeType;
import org.apache.kafka.common.serialization.Deserializer;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;

class SerdeErrorCaptureTest {

    private static Deserializer<Object> jsonDeserializer() {
        var json = (Deserializer<Object>) AbstractKafkaConnection.getTypedDeserializer(SerdeType.JSON);
        json.configure(Map.of(), false);
        return json;
    }

    @Test
    void capturesJsonErrorInsteadOfThrowing() {
        var capturing = AbstractKafkaConnection.captureSerdeErrors(jsonDeserializer());

        var value = capturing.deserialize("topic", "this is not json {".getBytes(StandardCharsets.UTF_8));

        assertThat(value, instanceOf(AbstractKafkaConnection.PluginKafkaSerdeException.class));
        assertThat(
            new String(((AbstractKafkaConnection.PluginKafkaSerdeException) value).getData(), StandardCharsets.UTF_8),
            is("this is not json {")
        );
    }

    @Test
    void passesThroughValidJson() {
        var capturing = AbstractKafkaConnection.captureSerdeErrors(jsonDeserializer());

        var value = capturing.deserialize("topic", "{\"a\":1}".getBytes(StandardCharsets.UTF_8));

        assertThat(value, not(instanceOf(AbstractKafkaConnection.PluginKafkaSerdeException.class)));
    }
}
