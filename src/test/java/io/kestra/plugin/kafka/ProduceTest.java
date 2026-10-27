package io.kestra.plugin.kafka;

import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;
import java.util.Map;
import java.util.List;
import java.util.HashMap;
import java.util.Collections;
import java.util.ArrayList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertNotNull;

@KestraTest
class ProduceTest {

    @Inject
    private RunContextFactory runContextFactory;

    @Test
    void testProduceEdgeCases() throws Exception {
        RunContext runContext = runContextFactory.of();

        Produce task = Produce.builder()
            .id("kafka_produce_test")
            .type(Produce.class.getName())
            .properties(Property.ofValue(Map.of("bootstrap.servers", "localhost:9092")))
            .topic(Property.ofValue("test-topic"))
            .build();

        java.lang.reflect.Method method = Produce.class.getDeclaredMethod("producerRecord", RunContext.class, org.apache.kafka.clients.producer.KafkaProducer.class, Map.class);
        method.setAccessible(true);
        
        // Test Long Partition and Null Map Header
        Map<String, Object> map1 = new HashMap<>();
        map1.put("topic", "test-topic");
        map1.put("partition", 0L);
        map1.put("headers", Collections.singletonMap("X-Marker", null));
        
        org.apache.kafka.clients.producer.KafkaProducer producerMock = 
            org.mockito.Mockito.mock(org.apache.kafka.clients.producer.KafkaProducer.class);
        
        org.apache.kafka.clients.producer.ProducerRecord<?, ?> record1 = 
            (org.apache.kafka.clients.producer.ProducerRecord<?, ?>) method.invoke(task, runContext, producerMock, map1);
        
        assertEquals(0, record1.partition());
        assertNull(record1.headers().lastHeader("X-Marker").value());

        // Test Empty Map in List Headers
        Map<String, Object> map2 = new HashMap<>();
        map2.put("topic", "test-topic");
        List<Map<String, Object>> listHeaders = new ArrayList<>();
        listHeaders.add(Collections.emptyMap()); 
        listHeaders.add(Collections.singletonMap("Valid", "Header"));
        map2.put("headers", listHeaders);

        org.apache.kafka.clients.producer.ProducerRecord<?, ?> record2 = 
            (org.apache.kafka.clients.producer.ProducerRecord<?, ?>) method.invoke(task, runContext, producerMock, map2);
        
        assertNotNull(record2.headers().lastHeader("Valid"));
    }
}