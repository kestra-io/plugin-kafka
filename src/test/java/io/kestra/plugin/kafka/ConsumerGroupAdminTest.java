package io.kestra.plugin.kafka;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.utils.IdUtils;
import io.kestra.plugin.kafka.ConsumerGroupAlterOffsets.TopicPartitionOffset;
import io.kestra.plugin.kafka.serdes.SerdeType;
import io.micronaut.context.annotation.Value;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;
import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.assertThrows;

@KestraTest
class ConsumerGroupAdminTest {

    @Inject
    private RunContextFactory runContextFactory;

    @Value("${kafka.bootstrap}")
    private String bootstrap;

    private Property<Map<String, String>> connection() {
        return Property.ofValue(Map.of("bootstrap.servers", this.bootstrap));
    }

    private String produceAndConsume(RunContext runContext, String groupId) throws Exception {
        String topic = "tu_admin_cg_" + IdUtils.create();

        Produce produce = Produce.builder()
            .properties(connection())
            .keySerializer(Property.ofValue(SerdeType.STRING))
            .valueSerializer(Property.ofValue(SerdeType.STRING))
            .topic(Property.ofValue(topic))
            .from(Map.of("key", "key", "value", "value"))
            .build();
        produce.run(runContext);

        Consume consume = Consume.builder()
            .properties(Property.ofValue(Map.of(
                "bootstrap.servers", this.bootstrap,
                "auto.offset.reset", "earliest",
                "max.poll.records", "15"
            )))
            .groupId(Property.ofValue(groupId))
            .keyDeserializer(Property.ofValue(SerdeType.STRING))
            .valueDeserializer(Property.ofValue(SerdeType.STRING))
            .pollDuration(Property.ofValue(Duration.ofSeconds(5)))
            .topic(topic)
            .build();
        consume.run(runContext);

        return topic;
    }

    @Test
    void listDescribeAlterAndDeleteConsumerGroup() throws Exception {
        RunContext runContext = runContextFactory.of(Map.of());
        String groupId = "tu_admin_group_" + IdUtils.create();
        String topic = produceAndConsume(runContext, groupId);

        ConsumerGroupList list = ConsumerGroupList.builder().properties(connection()).build();
        ConsumerGroupList.Output listOutput = list.run(runContext);
        assertThat(
            listOutput.getGroups().stream().map(g -> g.get("groupId")).toList(),
            hasItem(groupId)
        );

        ConsumerGroupDescribe describe = ConsumerGroupDescribe.builder()
            .properties(connection())
            .groupIds(Property.ofValue(List.of(groupId)))
            .build();
        ConsumerGroupDescribe.Output describeOutput = describe.run(runContext);
        assertThat(describeOutput.getGroups(), hasSize(1));
        @SuppressWarnings("unchecked")
        List<Map<String, Object>> offsets = (List<Map<String, Object>>) describeOutput.getGroups().getFirst().get("offsets");
        assertThat(offsets, hasSize(1));
        assertThat(offsets.getFirst().get("topic"), is(topic));
        assertThat(offsets.getFirst().get("lag"), is(0L));

        ConsumerGroupAlterOffsets alterOffsets = ConsumerGroupAlterOffsets.builder()
            .properties(connection())
            .groupId(Property.ofValue(groupId))
            .offsets(List.of(TopicPartitionOffset.builder()
                .topic(Property.ofValue(topic))
                .partition(Property.ofValue(0))
                .offset(Property.ofValue(0L))
                .build()))
            .build();
        ConsumerGroupAlterOffsets.Output alterOutput = alterOffsets.run(runContext);
        assertThat(alterOutput.getGroupId(), is(groupId));
        assertThat(alterOutput.getAlteredOffsets(), hasSize(1));

        ConsumerGroupDelete delete = ConsumerGroupDelete.builder()
            .properties(connection())
            .groupIds(Property.ofValue(List.of(groupId)))
            .build();
        ConsumerGroupDelete.Output deleteOutput = delete.run(runContext);
        assertThat(deleteOutput.getDeletedGroups(), contains(groupId));
    }

    @Test
    void shouldThrowWhenGroupIdsMissing() {
        RunContext runContext = runContextFactory.of(Map.of());

        ConsumerGroupDelete delete = ConsumerGroupDelete.builder()
            .properties(connection())
            .groupIds(Property.ofValue(List.of()))
            .build();

        assertThrows(IllegalArgumentException.class, () -> delete.run(runContext));
    }
}
