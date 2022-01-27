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
import org.apache.kafka.common.serialization.Serializer;
import org.slf4j.Logger;

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
        description = "Source of message:\n"+
            "Can be a unique entry, a file or a list\n" +
            "with the following format: key, value, timestamp, headers"
    )
    @NotNull
    @PluginProperty(dynamic = true)
    private Object from;

    @io.swagger.v3.oas.annotations.media.Schema(
        title = "Serializer use for the key",
        description = "Serializer use for the key, can be:\n"+
                "String, Integer, Float, Double, Long, Short, ByteArray, ByteBuffer, Bytes, UUID, Void, AVRO, JSON\n" +
                "List are not handled."
    )
    @NotNull
    @PluginProperty(dynamic = true)
    private String keySerializer;

    @io.swagger.v3.oas.annotations.media.Schema(
        title = "Serializer use for the value",
        description = "Serializer use for the value, can be:\n"+
                "String, Integer, Float, Double, Long, Short, ByteArray, ByteBuffer, Bytes, UUID, Void, AVRO, JSON\n" +
                "List are not handled."
    )
    @NotNull
    @PluginProperty(dynamic = true)
    private String valueSerializer;

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

    @Override
    public Output run(RunContext runContext) throws Exception {
        Logger logger = runContext.logger();
        Long count = 1L;

        Properties props = this.createProperties(this.properties, runContext);
        Properties config = this.createProperties(this.serializerConfig, runContext);

        Serializer keySerial = this.getTypedSerializer(this.keySerializer);
        Serializer valSerial = this.getTypedSerializer(this.valueSerializer);

        keySerial.configure(config, true);
        valSerial.configure(config, false);

        KafkaProducer producer = new KafkaProducer<>(props, keySerial, valSerial);

        if (this.from instanceof String || this.from instanceof List) {
            Flowable<Object> flowable;
            if (this.from instanceof String) {
                URI from = new URI(runContext.render((String) this.from));
                try (BufferedReader inputStream = new BufferedReader(new InputStreamReader(runContext.uriToInputStream(from))))
                {
                    flowable = Flowable.create(FileSerde.reader(inputStream), BackpressureStrategy.BUFFER);
                    flowable = this.buildFlowable(flowable, runContext, producer);
                    count = flowable
                        .count()
                        .blockingGet();
                }
            } else {
                flowable = Flowable.fromArray(((List) this.from).toArray());
                flowable = this.buildFlowable(flowable, runContext, producer);
                count = flowable
                    .count()
                    .blockingGet();
            }
        } else {
            Object key;
            Object value;

            Map<Object, Object> map = (Map) this.from;
            if (this.keySerializer == "AVRO") {
                key = buildAvroRecord(runContext, this.avroKeySchema, (Map) map.get("key"));
            } else {
                key = map.get("key");
            }
            if (this.valueSerializer == "AVRO") {
                value = buildAvroRecord(runContext, this.avroValueSchema, (Map) map.get("value"));
            } else {
                value = map.get("value");
            }
            producer.send(new ProducerRecord(this.topic, this.partition, (Long) map.get("timestamp"), key, value, (Iterable) map.get("headers")));
        }
        producer.flush();
        producer.close();

        return Output.builder().messageProduce(count).build();
    }

    private GenericRecord buildAvroRecord(RunContext runContext, String dataSchema, Map map) throws Exception {
        Schema.Parser parser = new Schema.Parser();
        Schema schema = parser.parse(runContext.render(dataSchema));
        GenericRecord avroRecord = new GenericData.Record(schema);
        Map<String, Object> keyMap = map;
        for (String k : keyMap.keySet()) {
            avroRecord.put(k, keyMap.get(k));
        }
        return avroRecord;
    }

    private Flowable buildFlowable(Flowable flowable, RunContext runContext, KafkaProducer producer){
        Flowable newFlowable = flowable.buffer(this.chunk, this.chunk)
            .map(list -> {
                Object key;
                Object value;
                for(Object row : (List) list) {
                    Map map = (Map) row;
                    if (this.keySerializer == "AVRO") {
                        key = buildAvroRecord(runContext, this.avroKeySchema, (Map) map.get("key"));
                    } else {
                        key = map.get("key");
                    }
                    if (this.valueSerializer == "AVRO") {
                        value = buildAvroRecord(runContext, this.avroValueSchema, (Map) map.get("value"));
                    } else {
                        value = map.get("value");
                    }
                    producer.send(new ProducerRecord(this.topic, this.partition, (Long) map.get("timestamp"), key, value, (Iterable) map.get("headers")));
                }
                return 0;
            });
        return newFlowable;
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @io.swagger.v3.oas.annotations.media.Schema(
            title = "Number of message produced"
        )
        private final Long messageProduce;
    }


}
