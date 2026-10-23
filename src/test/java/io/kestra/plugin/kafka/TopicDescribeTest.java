package io.kestra.plugin.kafka;

import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartitionInfo;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

class TopicDescribeTest {

    private static final Node LEADER = new Node(1, "broker-1", 9092);
    private static final Node FOLLOWER = new Node(2, "broker-2", 9092);

    @Test
    void partitionInfoWithoutOffsetsKeepsOriginalKeys() {
        TopicPartitionInfo info = new TopicPartitionInfo(0, LEADER, List.of(LEADER, FOLLOWER), List.of(LEADER));

        Map<String, Object> partition = TopicDescribe.partitionInfo(info, null, null);

        assertThat(partition.keySet(), contains("partition", "leader", "replicas", "isr"));
        assertThat(partition.get("partition"), is(0));
        assertThat(partition.get("leader"), is("1"));
        assertThat(partition.get("replicas"), is(List.of("1", "2")));
        assertThat(partition.get("isr"), is(List.of("1")));
    }

    @Test
    void partitionInfoWithOffsetsAddsEarliestAndLatest() {
        TopicPartitionInfo info = new TopicPartitionInfo(2, LEADER, List.of(LEADER), List.of(LEADER));

        Map<String, Object> partition = TopicDescribe.partitionInfo(info, 5L, 42L);

        assertThat(partition.keySet(), contains("partition", "leader", "replicas", "isr", "earliestOffset", "latestOffset"));
        assertThat(partition.get("earliestOffset"), is(5L));
        assertThat(partition.get("latestOffset"), is(42L));
    }

    @Test
    void partitionInfoWithoutLeaderYieldsNullLeader() {
        TopicPartitionInfo info = new TopicPartitionInfo(1, null, List.of(FOLLOWER), List.of());

        Map<String, Object> partition = TopicDescribe.partitionInfo(info, 0L, 0L);

        assertThat(partition.get("leader"), nullValue());
        assertThat(partition.get("isr"), is(List.of()));
        assertThat(partition.get("earliestOffset"), is(0L));
        assertThat(partition.get("latestOffset"), is(0L));
    }
}
