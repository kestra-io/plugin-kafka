package io.kestra.plugin.kafka;

import com.google.common.collect.ImmutableMap;
import io.kestra.core.models.executions.Execution;
import io.kestra.core.queues.QueueFactoryInterface;
import io.kestra.core.queues.QueueInterface;
import io.kestra.core.repositories.LocalFlowRepositoryLoader;
import io.kestra.core.runners.FlowListeners;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.runners.Worker;
import io.kestra.core.schedulers.AbstractScheduler;
import io.kestra.jdbc.runner.JdbcScheduler;
import io.kestra.core.utils.TestsUtils;
import io.kestra.plugin.kafka.serdes.SerdeType;
import io.micronaut.context.ApplicationContext;
import io.micronaut.context.annotation.Value;
import io.kestra.core.junit.annotations.KestraTest;
import jakarta.inject.Inject;
import jakarta.inject.Named;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

@KestraTest
class RealtimeTriggerTest {
    @Inject
    private ApplicationContext applicationContext;

    @Inject
    private FlowListeners flowListenersService;

    @Inject
    @Named(QueueFactoryInterface.EXECUTION_NAMED)
    private QueueInterface<Execution> executionQueue;

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
        CountDownLatch queue1Count = new CountDownLatch(2);
        CountDownLatch queue2Count = new CountDownLatch(2);

        // scheduler
        Worker worker = new Worker(applicationContext, 8, null);
        try (
            AbstractScheduler scheduler = new JdbcScheduler(
                this.applicationContext,
                this.flowListenersService
            );
        ) {
            List<Execution> executionList = new CopyOnWriteArrayList<>();

            // wait for execution
            executionQueue.receive(execution -> {
                executionList.add(execution.getLeft());

                if (queue1Count.getCount() == 0) {
                    queue2Count.countDown();
                } else {
                    queue1Count.countDown();
                }
                assertThat(execution.getLeft().getFlowId(), is("realtime"));
            });

            worker.run();
            scheduler.run();

            repositoryLoader.load(Objects.requireNonNull(RealtimeTriggerTest.class.getClassLoader().getResource("flows/realtime.yaml")));

            produce();
            queue1Count.await(1, TimeUnit.MINUTES);
            assertThat(executionList.size(), is(2));
            assertThat(executionList.stream().filter(execution -> execution.getTrigger().getVariables().get("key").equals("key1")).count(), is(1L));
            executionList.clear();

            produce();
            queue2Count.await(1, TimeUnit.MINUTES);
            assertThat(executionList.size(), is(2));
            assertThat(executionList.stream().filter(execution -> execution.getTrigger().getVariables().get("key").equals("key2")).count(), is(1L));
            executionList.clear();
        }
    }

    void produce() throws Exception {
        Produce task = Produce.builder()
            .id(RealtimeTriggerTest.class.getSimpleName())
            .type(Produce.class.getName())
            .properties(Map.of("bootstrap.servers", this.bootstrap))
            .serdeProperties(Map.of("schema.registry.url", this.registry))
            .keySerializer(SerdeType.STRING)
            .valueSerializer(SerdeType.STRING)
            .topic("tu_stream")
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

        task.run(TestsUtils.mockRunContext(runContextFactory, task, ImmutableMap.of()));
    }
}
