package io.kestra.plugin.kafka;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.utils.IdUtils;
import io.micronaut.context.annotation.Value;
import jakarta.inject.Inject;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.assertThrows;

@KestraTest
class TopicAdminTest {

    @Inject
    private RunContextFactory runContextFactory;

    @Value("${kafka.bootstrap}")
    private String bootstrap;

    private Property<Map<String, String>> connection() {
        return Property.ofValue(Map.of("bootstrap.servers", this.bootstrap));
    }

    @Test
    void createListDescribeAndDeleteTopic() throws Exception {
        RunContext runContext = runContextFactory.of(Map.of());
        String topic = "tu_admin_" + IdUtils.create();

        TopicCreate create = TopicCreate.builder()
            .properties(connection())
            .topic(Property.ofValue(topic))
            .partitions(Property.ofValue(3))
            .replicationFactor(Property.ofValue(1))
            .build();

        TopicCreate.Output createOutput = create.run(runContext);
        assertThat(createOutput.getTopic(), is(topic));
        assertThat(createOutput.getPartitions(), is(3));
        assertThat(createOutput.getReplicationFactor(), is(1));

        TopicList list = TopicList.builder().properties(connection()).build();
        TopicList.Output listOutput = list.run(runContext);
        assertThat(listOutput.getTopics(), hasItem(topic));

        TopicDescribe describe = TopicDescribe.builder().properties(connection()).topic(Property.ofValue(topic)).build();
        TopicDescribe.Output describeOutput = describe.run(runContext);
        assertThat(describeOutput.getPartitionCount(), is(3));
        assertThat(describeOutput.getPartitions(), hasSize(3));

        TopicCreatePartitions createPartitions = TopicCreatePartitions.builder()
            .properties(connection())
            .topic(Property.ofValue(topic))
            .totalPartitionCount(Property.ofValue(6))
            .build();
        TopicCreatePartitions.Output partitionsOutput = createPartitions.run(runContext);
        assertThat(partitionsOutput.getTotalPartitionCount(), is(6));

        TopicDelete delete = TopicDelete.builder().properties(connection()).topics(Property.ofValue(List.of(topic))).build();
        TopicDelete.Output deleteOutput = delete.run(runContext);
        assertThat(deleteOutput.getDeletedTopics(), contains(topic));
    }

    @Test
    void shouldUpdateTopicRetention() throws Exception {
        RunContext runContext = runContextFactory.of(Map.of());
        String topic = "tu_admin_" + IdUtils.create();

        TopicCreate.builder()
            .properties(connection())
            .topic(Property.ofValue(topic))
            .partitions(Property.ofValue(1))
            .build()
            .run(runContext);

        TopicUpdate update = TopicUpdate.builder()
            .properties(connection())
            .topic(Property.ofValue(topic))
            .retentionMs(Property.ofValue(3600000L))
            .build();
        TopicUpdate.Output updateOutput = update.run(runContext);
        assertThat(updateOutput.getUpdatedConfigs().get("retention.ms"), is("3600000"));

        TopicDescribe describe = TopicDescribe.builder().properties(connection()).topic(Property.ofValue(topic)).build();
        TopicDescribe.Output describeOutput = describe.run(runContext);
        assertThat(describeOutput.getConfigs().get("retention.ms"), is("3600000"));

        TopicDelete.builder().properties(connection()).topics(Property.ofValue(List.of(topic))).build().run(runContext);
    }

    @Test
    void shouldFailWhenTopicAlreadyExists() throws Exception {
        RunContext runContext = runContextFactory.of(Map.of());
        String topic = "tu_admin_" + IdUtils.create();

        TopicCreate create = TopicCreate.builder()
            .properties(connection())
            .topic(Property.ofValue(topic))
            .partitions(Property.ofValue(1))
            .build();
        create.run(runContext);

        try {
            assertThrows(TopicExistsException.class, () -> create.run(runContext));
        } finally {
            TopicDelete.builder().properties(connection()).topics(Property.ofValue(List.of(topic))).build().run(runContext);
        }
    }

    @Test
    void shouldBeIdempotentWhenIfNotExists() throws Exception {
        RunContext runContext = runContextFactory.of(Map.of());
        String topic = "tu_admin_" + IdUtils.create();

        TopicCreate create = TopicCreate.builder()
            .properties(connection())
            .topic(Property.ofValue(topic))
            .partitions(Property.ofValue(1))
            .ifNotExists(Property.ofValue(true))
            .build();
        TopicCreate.Output firstRun = create.run(runContext);
        assertThat(firstRun.getCreated(), is(true));

        try {
            TopicCreate.Output secondRun = create.run(runContext);
            assertThat(secondRun.getTopic(), is(topic));
            assertThat(secondRun.getCreated(), is(false));
            assertThat(secondRun.getPartitions(), is(1));
        } finally {
            TopicDelete.builder().properties(connection()).topics(Property.ofValue(List.of(topic))).build().run(runContext);
        }
    }

    @Test
    void shouldReportActualShapeWhenSkippingExistingTopicWithDifferentRequestedShape() throws Exception {
        RunContext runContext = runContextFactory.of(Map.of());
        String topic = "tu_admin_" + IdUtils.create();

        TopicCreate.builder()
            .properties(connection())
            .topic(Property.ofValue(topic))
            .partitions(Property.ofValue(1))
            .replicationFactor(Property.ofValue(1))
            .build()
            .run(runContext);

        try {
            TopicCreate reCreate = TopicCreate.builder()
                .properties(connection())
                .topic(Property.ofValue(topic))
                .partitions(Property.ofValue(9))
                .replicationFactor(Property.ofValue(3))
                .ifNotExists(Property.ofValue(true))
                .build();
            TopicCreate.Output output = reCreate.run(runContext);

            assertThat(output.getCreated(), is(false));
            assertThat(output.getPartitions(), is(1));
            assertThat(output.getReplicationFactor(), is(1));
        } finally {
            TopicDelete.builder().properties(connection()).topics(Property.ofValue(List.of(topic))).build().run(runContext);
        }
    }

    @Test
    void shouldSurfaceUnknownTopicOnDescribe() {
        RunContext runContext = runContextFactory.of(Map.of());

        TopicDescribe describe = TopicDescribe.builder()
            .properties(connection())
            .topic(Property.ofValue("tu_admin_missing_" + IdUtils.create()))
            .build();

        assertThrows(UnknownTopicOrPartitionException.class, () -> describe.run(runContext));
    }

    @Test
    void shouldThrowWhenTopicMissing() {
        RunContext runContext = runContextFactory.of(Map.of());

        TopicCreate create = TopicCreate.builder()
            .properties(connection())
            .partitions(Property.ofValue(1))
            .build();

        assertThrows(IllegalArgumentException.class, () -> create.run(runContext));
    }

    @Test
    void shouldDescribeLogDirs() throws Exception {
        RunContext runContext = runContextFactory.of(Map.of());
        String topic = "tu_admin_" + IdUtils.create();

        TopicCreate.builder()
            .properties(connection())
            .topic(Property.ofValue(topic))
            .partitions(Property.ofValue(1))
            .build()
            .run(runContext);

        try {
            DescribeLogDirs describeLogDirs = DescribeLogDirs.builder().properties(connection()).build();
            DescribeLogDirs.Output output = describeLogDirs.run(runContext);

            assertThat(output.getLogDirs(), not(empty()));
        } finally {
            TopicDelete.builder().properties(connection()).topics(Property.ofValue(List.of(topic))).build().run(runContext);
        }
    }
}
