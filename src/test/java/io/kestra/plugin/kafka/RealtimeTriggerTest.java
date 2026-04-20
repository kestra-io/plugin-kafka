package io.kestra.plugin.kafka;


import com.google.common.collect.ImmutableMap;
import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.executions.Execution;
import io.kestra.core.models.property.Property;
import io.kestra.core.queues.DispatchQueueInterface;
import io.kestra.core.repositories.LocalFlowRepositoryLoader;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.utils.TestsUtils;
import io.kestra.plugin.kafka.serdes.SerdeType;
import io.micronaut.context.annotation.Value;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

@KestraTest(startRunner = true, startScheduler = true)
class RealtimeTriggerTest {
    @Inject
    private DispatchQueueInterface<Execution> executionQueue;

    @Inject
    protected LocalFlowRepositoryLoader repositoryLoader;
    @Inject
    private RunContextFactory runContextFactory;

    @Value("${kafka.bootstrap}")
    private String bootstrap;

    @Value("${kafka.registry}")
    private String registry;

    @Test
    void flow() throws Exception {
        var queue1Count = new CountDownLatch(2);
        var queue2Count = new CountDownLatch(2);
        var executionList = new CopyOnWriteArrayList<Execution>();

        executionQueue.addListener(execution -> {
            executionList.add(execution);

            if (queue1Count.getCount() == 0) {
                queue2Count.countDown();
            } else {
                queue1Count.countDown();
            }
            assertThat(execution.getFlowId(), is("realtime"));
        });

        repositoryLoader.load(
            Objects.requireNonNull(
                RealtimeTriggerTest.class.getClassLoader().getResource("flows/realtime.yaml")
            )
        );

        produce();
        assertThat(queue1Count.await(1, TimeUnit.MINUTES), is(true));
        assertThat(executionList.size(), is(2));
        assertThat(executionList.stream()
            .filter(execution -> execution.getTrigger().getVariables().get("key").equals("key1"))
            .count(), is(1L));
        executionList.clear();

        produce();
        assertThat(queue2Count.await(1, TimeUnit.MINUTES), is(true));
        assertThat(executionList.size(), is(2));
        assertThat(executionList.stream()
            .filter(execution -> execution.getTrigger().getVariables().get("key").equals("key2"))
            .count(), is(1L));
    }

    void produce() throws Exception {
        Produce task = Produce.builder()
            .id(RealtimeTriggerTest.class.getSimpleName())
            .type(Produce.class.getName())
            .properties(Property.ofValue(Map.of("bootstrap.servers", this.bootstrap)))
            .serdeProperties(Property.ofValue(Map.of("schema.registry.url", this.registry)))
            .keySerializer(Property.ofValue(SerdeType.STRING))
            .valueSerializer(Property.ofValue(SerdeType.STRING))
            .topic(Property.ofValue("tu_stream"))
            .from(List.of(
                ImmutableMap.builder()
                    .put("key", "key1")
                    .put("value", "value1")
                    .build(),
                ImmutableMap.builder()
                    .put("key", "key2")
                    .put("value", "value2")
                    .build()
            ))
            .build();

        task.run(TestsUtils.mockRunContext(runContextFactory, task, Map.of()));
    }
}
