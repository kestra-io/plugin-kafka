package io.kestra.plugin.kafka;

import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.kestra.core.serializers.FileSerde;
import io.reactivex.BackpressureStrategy;
import io.reactivex.Flowable;
import lombok.*;
import lombok.experimental.SuperBuilder;


import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.serialization.Serializer;

import javax.validation.constraints.NotNull;

import java.io.*;
import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.Properties;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@io.swagger.v3.oas.annotations.media.Schema(
    title = "Produce message in a Kafka topic"
)
public class Produce extends AbstractKafkaConnection implements RunnableTask<Produce.Output> {
    @io.swagger.v3.oas.annotations.media.Schema(
        title = "Kafka topic where to send message"
    )
    @NotNull
    @PluginProperty(dynamic = true)
    private String topic;

    @io.swagger.v3.oas.annotations.media.Schema(
        title = "Kafka partition where to send message"
    )
    @NotNull
    @PluginProperty(dynamic = true)
    private Integer partition;

    @io.swagger.v3.oas.annotations.media.Schema(
        title = "Message headers"
    )
    @NotNull
    @PluginProperty(dynamic = true)
    private Map<String, String> headers;

    @io.swagger.v3.oas.annotations.media.Schema(
        title = "Source of message send",
        description = "Source of message:\n" +
            "Can be a unique entry, a file or a list\n" +
            "with the following format: key, value, timestamp, headers"
    )
    @NotNull
    @PluginProperty(dynamic = true)
    private Object from;


    @io.swagger.v3.oas.annotations.media.Schema(
        title = "Serializer use for the key",
        description = "Serializer use for the key, can be:\n" +
            "String, Integer, Float, Double, Long, Short, ByteArray, ByteBuffer, Bytes, UUID, Void, AVRO, JSON\n" +
            "List are not handled."
    )
    @NotNull
    @PluginProperty(dynamic = true)
    private SerializerType keySerializer;

    @io.swagger.v3.oas.annotations.media.Schema(
        title = "Serializer use for the value",
        description = "Serializer use for the value, can be:\n" +
            "String, Integer, Float, Double, Long, Short, ByteArray, ByteBuffer, Bytes, UUID, Void, AVRO, JSON\n" +
            "List are not handled."
    )
    @NotNull
    @PluginProperty(dynamic = true)
    private SerializerType valueSerializer;

    @io.swagger.v3.oas.annotations.media.Schema(
        title = "Schema if key is AVRO type"
    )
    @PluginProperty(dynamic = true)
    private String avroKeySchema;

    @io.swagger.v3.oas.annotations.media.Schema(
        title = "Schema if value is AVRO type"
    )
    @PluginProperty(dynamic = true)
    private String avroValueSchema;

    @io.swagger.v3.oas.annotations.media.Schema(
        title = "The size of chunk for every bulk request"
    )
    @PluginProperty(dynamic = true)
    @Builder.Default
    private Integer chunk = 50;

    @SuppressWarnings("unchecked")
    @Override
    public Output run(RunContext runContext) throws Exception {

        Properties props = this.createProperties(this.properties, runContext);
        Properties config = this.createProperties(this.serializerConfig, runContext);

        Serializer keySerial = this.getTypedSerializer(this.keySerializer);
        Serializer valSerial = this.getTypedSerializer(this.valueSerializer);

        keySerial.configure(config, true);
        valSerial.configure(config, false);

        KafkaProducer<Object, Object> producer = new KafkaProducer<Object, Object>(props, keySerial, valSerial);

        Integer count = 1;

        if (this.from instanceof String || this.from instanceof List) {
            Flowable<Object> flowable;
            Flowable<Integer> resultFlowable;
            if (this.from instanceof String) {
                URI from = new URI(runContext.render((String) this.from));
                try (BufferedReader inputStream = new BufferedReader(new InputStreamReader(runContext.uriToInputStream(from)))) {
                    flowable = Flowable.create(FileSerde.reader(inputStream), BackpressureStrategy.BUFFER);
                    resultFlowable = this.buildFlowable(flowable, runContext, producer);

                    count = resultFlowable
                        .reduce(Integer::sum)
                        .blockingGet();
                }
            } else {
                flowable = Flowable.fromArray(((List<Object>) this.from).toArray());
                resultFlowable = this.buildFlowable(flowable, runContext, producer);

                count = resultFlowable
                    .reduce(Integer::sum)
                    .blockingGet();
            }
        } else {
            Object key;
            Object value;

            Map<String, Object> map = (Map<String, Object>) this.from;
            if (this.keySerializer == SerializerType.AVRO) {
                key = buildAvroRecord(runContext, this.avroKeySchema, (Map<String, Object>) map.get("key"));
            } else {
                key = map.get("key");
            }
            if (this.valueSerializer == SerializerType.AVRO) {
                value = buildAvroRecord(runContext, this.avroValueSchema, (Map<String, Object>) map.get("value"));
            } else {
                value = map.get("value");
            }
            producer.send(new ProducerRecord<>(this.topic, this.partition, (Long) map.get("timestamp"), key, value, (Iterable<Header>) map.get("headers")));
        }
        producer.flush();
        producer.close();

        return Output.builder().messageProduce(count).build();
    }

    private GenericRecord buildAvroRecord(RunContext runContext, String dataSchema, Map<String, Object> map) throws Exception {
        Schema.Parser parser = new Schema.Parser();
        Schema schema = parser.parse(runContext.render(dataSchema));
        GenericRecord avroRecord = new GenericData.Record(schema);
        for (String k : map.keySet()) {
            avroRecord.put(k, map.get(k));
        }
        return avroRecord;
    }

    @SuppressWarnings("unchecked")
    private Flowable<Integer> buildFlowable(Flowable<Object> flowable, RunContext runContext, KafkaProducer<Object, Object> producer) {
        Flowable<Integer> newFlowable;
        newFlowable = flowable.buffer(this.chunk, this.chunk)
            .map(list -> {
                Object key;
                Object value;
                for (Object row : list) {
                    Map<String, Object> map = (Map<String, Object>) row;
                    if (this.keySerializer == SerializerType.AVRO) {
                        key = buildAvroRecord(runContext, this.avroKeySchema, (Map<String, Object>) map.get("key"));
                    } else {
                        key = map.get("key");
                    }
                    if (this.valueSerializer == SerializerType.AVRO) {
                        value = buildAvroRecord(runContext, this.avroValueSchema, (Map<String, Object>) map.get("value"));
                    } else {
                        value = map.get("value");
                    }
                    producer.send(new ProducerRecord<>(this.topic, this.partition, (Long) map.get("timestamp"), key, value, (Iterable<Header>) map.get("headers")));
                }
                return list.size();
            });
        return newFlowable;
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @io.swagger.v3.oas.annotations.media.Schema(
            title = "Number of message produced"
        )
        private final Integer messageProduce;
    }


}
