package io.kestra.plugin.kafka;

import com.google.common.collect.ImmutableMap;
import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.utils.IdUtils;
import io.kestra.plugin.kafka.serdes.SerdeType;
import io.micronaut.context.annotation.Value;
import jakarta.inject.Inject;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.scheduler.Schedulers;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.startsWith;

@KestraTest
class RealtimeTriggerSerdeErrorTest {

    @Inject
    private RunContextFactory runContextFactory;

    @Value("${kafka.bootstrap}")
    private String bootstrap;

    private void createTopic(String name) {
        var props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        try (var admin = AdminClient.create(props)) {
            admin.createTopics(List.of(new NewTopic(name, 1, (short) 1))).all().get(30, TimeUnit.SECONDS);
        } catch (Exception e) {
            throw new RuntimeException("failed to create topic " + name, e);
        }
    }

    private void produceRaw(String topic, String value, String key) throws Exception {
        var builder = ImmutableMap.<String, String>builder().put("value", value);
        if (key != null) {
            builder.put("key", key);
        }
        var task = Produce.builder()
            .id(IdUtils.create())
            .type(Produce.class.getName())
            .properties(Property.ofValue(Map.of("bootstrap.servers", bootstrap)))
            .keySerializer(Property.ofValue(SerdeType.STRING))
            .valueSerializer(Property.ofValue(SerdeType.STRING))
            .topic(Property.ofValue(topic))
            .from(List.of(builder.build()))
            .build();
        task.run(runContextFactory.of(Map.of()));
    }

    private ConsumerRecord<String, String> consumeFirst(String topic) {
        var props = new Properties();
        props.put("bootstrap.servers", bootstrap);
        props.put("group.id", "tu_serde_reader_" + IdUtils.create());
        props.put("auto.offset.reset", "earliest");
        props.put("enable.auto.commit", "false");
        props.put("key.deserializer", StringDeserializer.class.getName());
        props.put("value.deserializer", StringDeserializer.class.getName());
        try (var consumer = new KafkaConsumer<String, String>(props)) {
            consumer.subscribe(List.of(topic));
            var deadline = System.currentTimeMillis() + TimeUnit.SECONDS.toMillis(30);
            while (System.currentTimeMillis() < deadline) {
                var records = consumer.poll(Duration.ofMillis(500));
                if (!records.isEmpty()) {
                    return records.iterator().next();
                }
            }
            return null;
        }
    }

    private RealtimeTrigger trigger(String topic, KafkaConsumerInterface.OnSerdeErrorBehavior behavior, String dlqTopic) {
        var builder = KafkaConsumerInterface.OnSerdeError.builder()
            .type(Property.ofValue(behavior));
        if (dlqTopic != null) {
            builder.topic(Property.ofValue(dlqTopic));
        }

        return RealtimeTrigger.builder()
            .id(IdUtils.create())
            .type(RealtimeTrigger.class.getName())
            .topic(topic)
            .groupId(Property.ofValue("tu_serde_group_" + IdUtils.create()))
            .properties(Property.ofValue(Map.of("bootstrap.servers", bootstrap, "auto.offset.reset", "earliest")))
            .keyDeserializer(Property.ofValue(SerdeType.STRING))
            .valueDeserializer(Property.ofValue(SerdeType.JSON))
            .onSerdeError(builder.build())
            .build();
    }

    @Test
    void shouldSkipMalformedJsonAndKeepConsuming() throws Exception {
        var topic = "tu_serde_" + IdUtils.create();
        createTopic(topic);
        produceRaw(topic, "this is not json {", null);
        produceRaw(topic, "{\"value\":\"value1\"}", null);

        var received = new CopyOnWriteArrayList<ConsumerRecord<Object, Object>>();
        var failure = new AtomicReference<Throwable>();
        var runContext = runContextFactory.of(Map.of());
        var t = trigger(topic, KafkaConsumerInterface.OnSerdeErrorBehavior.SKIPPED, null);

        Flux.from(t.publisher(t.consumeTask(), runContext))
            .subscribeOn(Schedulers.boundedElastic())
            .subscribe(received::add, failure::set);

        var deadline = System.currentTimeMillis() + TimeUnit.SECONDS.toMillis(30);
        while (received.isEmpty() && failure.get() == null && System.currentTimeMillis() < deadline) {
            Thread.sleep(200);
        }
        t.stop();

        assertThat("trigger must not fail on a malformed record", failure.get(), is(nullValue()));
        assertThat("malformed record is skipped and only the valid record is emitted", received.size(), is(1));
        assertThat("valid record is deserialized to a map", received.getFirst().value(), is(instanceOf(Map.class)));
    }

    @Test
    void shouldSendMalformedRecordToDlq() throws Exception {
        var topic = "tu_serde_dlq_" + IdUtils.create();
        var dlq = topic + "_dlq";
        createTopic(topic);
        createTopic(dlq);
        produceRaw(topic, "this is not json {", "key-1");
        produceRaw(topic, "{\"value\":\"value1\"}", "key-2");

        var received = new CopyOnWriteArrayList<ConsumerRecord<Object, Object>>();
        var failure = new AtomicReference<Throwable>();
        var runContext = runContextFactory.of(Map.of());
        var t = trigger(topic, KafkaConsumerInterface.OnSerdeErrorBehavior.DLQ, dlq);

        Flux.from(t.publisher(t.consumeTask(), runContext))
            .subscribeOn(Schedulers.boundedElastic())
            .subscribe(received::add, failure::set);

        var deadline = System.currentTimeMillis() + TimeUnit.SECONDS.toMillis(30);
        while (received.isEmpty() && failure.get() == null && System.currentTimeMillis() < deadline) {
            Thread.sleep(200);
        }
        Thread.sleep(2000);
        t.stop();

        assertThat("trigger must not fail on a malformed record", failure.get(), is(nullValue()));
        assertThat("only the valid record is emitted", received.size(), is(1));
        assertThat(received.getFirst().value(), is(instanceOf(Map.class)));

        var dlqRecord = consumeFirst(dlq);
        assertThat("malformed record is routed to the DLQ", dlqRecord.value(), is("this is not json {"));
        assertThat("DLQ record keeps the source key", dlqRecord.key(), is("key-1"));
    }

    @Test
    void shouldStoreMalformedRecord() throws Exception {
        var topic = "tu_serde_store_" + IdUtils.create();
        createTopic(topic);
        produceRaw(topic, "this is not json {", null);

        var received = new CopyOnWriteArrayList<ConsumerRecord<Object, Object>>();
        var failure = new AtomicReference<Throwable>();
        var runContext = runContextFactory.of(Map.of());
        var t = trigger(topic, KafkaConsumerInterface.OnSerdeErrorBehavior.STORE, null);

        Flux.from(t.publisher(t.consumeTask(), runContext))
            .subscribeOn(Schedulers.boundedElastic())
            .subscribe(received::add, failure::set);

        var deadline = System.currentTimeMillis() + TimeUnit.SECONDS.toMillis(30);
        while (received.isEmpty() && failure.get() == null && System.currentTimeMillis() < deadline) {
            Thread.sleep(200);
        }
        t.stop();

        assertThat("trigger must not fail on a malformed record", failure.get(), is(nullValue()));
        assertThat("STORE emits one execution with the stored URI", received.size(), is(1));
        assertThat("stored value is a Kestra internal-storage URI", received.getFirst().value(), is(instanceOf(URI.class)));
        assertThat(received.getFirst().value().toString(), startsWith("kestra://"));

        var uri = (URI) received.getFirst().value();
        try (var in = runContext.storage().getFile(uri)) {
            assertThat(new String(in.readAllBytes(), StandardCharsets.UTF_8), is("this is not json {"));
        }
    }
}
