package io.kestra.plugin.kafka;

import io.confluent.kafka.serializers.KafkaAvroSerializerConfig;
import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.executions.metrics.Counter;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.kestra.core.serializers.FileSerde;
import io.kestra.plugin.kafka.serdes.SerdeType;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.*;
import lombok.experimental.SuperBuilder;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Deserializer;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.net.URI;
import java.time.Duration;
import java.time.Instant;
import java.time.ZonedDateTime;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import javax.validation.constraints.NotNull;

import static io.kestra.core.utils.Rethrow.throwConsumer;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Produce message in a Kafka topic"
)
@Plugin(
    examples = {
        @Example(
            code = {
                "topic: test_kestra",
                "properties:",
                "  bootstrap.servers: local:9092",
                "serdeProperties:",
                "  schema.registry.url: http://local:8085",
                "keySerializer: STRING",
                "valueSerializer: AVRO\n",
                "valueAvroSchema: |",
                "  {\"type\":\"record\",\"name\":\"twitter_schema\",\"namespace\":\"io.kestra.examples\",\"fields\":[{\"name\":\"username\",\"type\":\"string\"},{\"name\":\"tweet\",\"type\":\"string\"}]}"
            }
        )
    }
)
public class Consume extends AbstractKafkaConnection implements RunnableTask<Consume.Output> {
    @Schema(
        title = "Kafka topic where to send message",
        description = "Can be a string or a List of string to consume from multiple topic"
    )
    @NotNull
    @PluginProperty(dynamic = true)
    private Object topic;

    @Schema(
        title = "The consumer group",
        description = "Using consumer group, we will fetch only records not already consumed"
    )
    @PluginProperty(dynamic = true)
    private String groupId;

    @Schema(
        title = "Serializer used for the key"
    )
    @NotNull
    @PluginProperty(dynamic = true)
    private SerdeType keyDeserializer;

    @Schema(
        title = "Serializer used for the value"
    )
    @NotNull
    @PluginProperty(dynamic = true)
    private SerdeType valueDeserializer;

    @Schema(
        title = "Timestamp of message to start with",
        description = "By default, we consume all messages from the topics with no consumer group or depending on " +
            "configuration `auto.offset.reset` with consumer group, but you can provide a arbitrary start time.\n" +
            "This property is ignore if a consumer group is used.\n" +
            "Must be a valid iso 8601 date."
    )
    @PluginProperty(dynamic = true)
    private String since;

    @Schema(
        title = "Duration waiting for record to be polled",
        description = "If no records are available, the max wait to wait for a new records. "
    )
    @NotNull
    @Builder.Default
    @PluginProperty(dynamic = true)
    private Duration pollDuration = Duration.ofSeconds(2);

    @SuppressWarnings({"unchecked", "rawtypes"})
    @Override
    public Output run(RunContext runContext) throws Exception {
        // ugly hack to force use of Kestra plugins classLoader
        Thread.currentThread().setContextClassLoader(this.getClass().getClassLoader());

        Properties properties = createProperties(this.properties, runContext);
        if (this.groupId != null) {
            properties.put(ConsumerConfig.GROUP_ID_CONFIG, runContext.render(groupId));
        }

        Properties serdesProperties = createProperties(this.serdeProperties, runContext);
        serdesProperties.put(KafkaAvroSerializerConfig.AVRO_USE_LOGICAL_TYPE_CONVERTERS_CONFIG, true);

        Deserializer keySerial = getTypedDeserializer(this.keyDeserializer);
        Deserializer valSerial = getTypedDeserializer(this.valueDeserializer);

        keySerial.configure(serdesProperties, true);
        valSerial.configure(serdesProperties, false);

        File tempFile = runContext.tempFile(".ion").toFile();
        try(
            BufferedOutputStream output = new BufferedOutputStream(new FileOutputStream(tempFile));
            KafkaConsumer<Object, Object> consumer = new KafkaConsumer<Object, Object>(properties, keySerial, valSerial);
        ) {
            this.affectTopics(runContext, consumer);

            Map<String, Integer> count = new HashMap<>();
            ConsumerRecords<Object, Object> records;
            boolean empty;

            do {
                records = consumer.poll(this.pollDuration);
                empty = records.isEmpty();

                records.forEach(throwConsumer(record -> {
                    // using HashMap for null values
                    Map<Object, Object> map = new HashMap<>();

                    map.put("key", record.key());
                    map.put("value", record.value());
                    map.put("headers", processHeaders(record.headers()));
                    map.put("topic", record.topic());
                    map.put("partition", record.partition());
                    map.put("timestamp", Instant.ofEpochMilli(record.timestamp()));

                    FileSerde.write(output, map);

                    count.compute(record.topic(), (s, integer) -> integer == null ? 1 : integer + 1);
                }));
            }
            while (!empty);

            if (this.groupId != null) {
                consumer.commitSync();
            }

            consumer.close();
            output.flush();

            count
                .forEach((s, integer) -> runContext.metric(Counter.of("records", integer, "topic", s)));

            return Output.builder()
                .messagesCount(count.values().stream().mapToInt(Integer::intValue).sum())
                .uri(runContext.putTempFile(tempFile))
                .build();
        }
    }

    @SuppressWarnings("unchecked")
    private void affectTopics(RunContext runContext, KafkaConsumer<Object, Object> consumer) throws IllegalVariableEvaluationException {
        Collection<String> topics;
        if (this.topic instanceof String) {
            topics = List.of(runContext.render((String) this.topic));
        } else if (this.topic instanceof List) {
            topics = runContext.render((List<String>) this.topic);
        } else {
            throw new IllegalArgumentException("Invalid topics with type '" + this.topic.getClass().getName() + "'");
        }

        if (this.groupId != null) {
            consumer.subscribe(topics);
        } else {
            List<TopicPartition> partitions = topics
                .stream()
                .flatMap(s -> consumer.partitionsFor(s).stream())
                .map(info -> new TopicPartition(info.topic(), info.partition()))
                .collect(Collectors.toList());

            consumer.assign(partitions);

            if (this.since == null) {
                consumer.seekToBeginning(partitions);
            } else {
                Long timestamp = ZonedDateTime
                    .parse(runContext.render(this.since))
                    .toInstant()
                    .toEpochMilli();

                consumer
                    .offsetsForTimes(
                        partitions.stream()
                            .map(topicPartition -> new AbstractMap.SimpleEntry<>(
                                topicPartition,
                                timestamp
                            ))
                            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue))
                    )
                    .forEach((topicPartition, offsetAndTimestamp) -> {
                        consumer.seek(topicPartition, offsetAndTimestamp.timestamp());
                    });
            }
        }
    }

    private static List<Pair<String, String>> processHeaders(Headers headers) {
         return StreamSupport
            .stream(headers.spliterator(), false)
            .map(header -> Pair.of(header.key(), Arrays.toString(header.value())))
            .collect(Collectors.toList());
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(
            title = "Number of message produced"
        )
        private final Integer messagesCount;

        @Schema(
            title = "URI of a kestra internal storage file"
        )
        private URI uri;
    }
}
