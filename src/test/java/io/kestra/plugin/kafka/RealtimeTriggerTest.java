package io.kestra.plugin.kafka;


import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import com.google.common.collect.ImmutableMap;
import io.kestra.core.models.executions.Execution;
import io.kestra.core.models.property.Property;
import io.kestra.core.queues.QueueFactoryInterface;
import io.kestra.core.queues.QueueInterface;
import io.kestra.core.repositories.LocalFlowRepositoryLoader;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.utils.IdUtils;
import io.kestra.core.utils.TestsUtils;
import io.kestra.plugin.kafka.serdes.SerdeType;
import io.micronaut.context.annotation.Value;
import io.kestra.core.junit.annotations.KestraTest;
import jakarta.inject.Inject;
import jakarta.inject.Named;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;
import org.reactivestreams.Publisher;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.scheduler.Schedulers;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

@KestraTest(startRunner = true, startScheduler = true)
class RealtimeTriggerTest {
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
        List<Execution> executionList = new CopyOnWriteArrayList<>();
        Flux<Execution> receive = TestsUtils.receive(executionQueue, execution -> {
            executionList.add(execution.getLeft());

            if (queue1Count.getCount() == 0) {
                queue2Count.countDown();
            } else {
                queue1Count.countDown();
            }
            assertThat(execution.getLeft().getFlowId(), is("realtime"));
        });

        repositoryLoader.load(Objects.requireNonNull(RealtimeTriggerTest.class.getClassLoader()
            .getResource("flows/realtime.yaml")));

        produce();
        boolean await = queue1Count.await(1, TimeUnit.MINUTES);
        assertThat(await, is(true));
        assertThat(executionList.size(), greaterThanOrEqualTo(2));
        assertThat(executionList.stream()
            .anyMatch(execution -> "key1".equals(execution.getTrigger().getVariables().get("key"))), is(true));
        executionList.clear();

        produce();
        await = queue2Count.await(1, TimeUnit.MINUTES);
        assertThat(await, is(true));
        assertThat(executionList.size(), greaterThanOrEqualTo(2));
        assertThat(executionList.stream()
            .anyMatch(execution -> "key2".equals(execution.getTrigger().getVariables().get("key"))), is(true));
        receive.blockLast();
    }

    @Test
    void shouldEmitStartupAndSubscriptionLogs() throws Exception {
        var triggerId = IdUtils.create();
        var topic = "tu_logs_" + IdUtils.create();

        var trigger = RealtimeTrigger.builder()
            .id(triggerId)
            .type(RealtimeTrigger.class.getName())
            .topic(topic)
            .groupId(Property.ofValue("test-group-" + IdUtils.create()))
            .properties(Property.ofValue(Map.of(
                "bootstrap.servers", this.bootstrap,
                "auto.offset.reset", "earliest"
            )))
            .serdeProperties(Property.ofValue(Map.of("schema.registry.url", this.registry)))
            .keyDeserializer(Property.ofValue(SerdeType.STRING))
            .valueDeserializer(Property.ofValue(SerdeType.STRING))
            .build();

        RunContext runContext = runContextFactory.of(Map.of());

        // runContext.logger() returns a Logback logger from an isolated LoggerContext inside RunContextLogger.
        // Cast it to attach our ListAppender directly, ensuring we capture logs regardless of global config.
        var contextLogger = (Logger) runContext.logger();
        contextLogger.setLevel(Level.INFO);
        var listAppender = new ListAppender<ILoggingEvent>();
        listAppender.setContext(contextLogger.getLoggerContext());
        listAppender.start();
        contextLogger.addAppender(listAppender);

        try {
            Consume task = trigger.consumeTask();
            Publisher<ConsumerRecord<Object, Object>> publisher = trigger.publisher(task, runContext);

            // subscribeOn puts the blocking Flux.create lambda on a background thread
            Flux.from(publisher)
                .subscribeOn(Schedulers.boundedElastic())
                .subscribe(record -> {}, err -> {});

            // Poll until the "Subscribed for triggerId" log appears (emitted before the first blocking poll)
            var deadline = System.currentTimeMillis() + TimeUnit.SECONDS.toMillis(30);
            var found = false;
            while (System.currentTimeMillis() < deadline) {
                found = listAppender.list.stream()
                    .map(ILoggingEvent::getFormattedMessage)
                    .anyMatch(m -> m.contains("Subscribed for triggerId=") && m.contains(triggerId));
                if (found) {
                    break;
                }
                Thread.sleep(200);
            }

            trigger.stop();

            assertThat("Timed out waiting for subscription log", found, is(true));

            var logMessages = listAppender.list.stream()
                .map(ILoggingEvent::getFormattedMessage)
                .toList();

            assertThat(
                "Startup log must contain triggerId",
                logMessages.stream().anyMatch(m -> m.contains("triggerId=" + triggerId)),
                is(true)
            );
            assertThat(
                "Startup log must include bootstrap.servers value",
                logMessages.stream().anyMatch(m -> m.contains("bootstrap.servers=") && m.contains(this.bootstrap)),
                is(true)
            );
            assertThat(
                "Subscription log must contain the configured topic name",
                logMessages.stream().anyMatch(m -> m.contains("Subscribed for triggerId=") && m.contains(topic)),
                is(true)
            );
            assertThat(
                "Consumer creation log must be present with CONSUMER group type",
                logMessages.stream().anyMatch(m -> m.contains("Kafka consumer created for triggerId=") && m.contains("groupType=CONSUMER")),
                is(true)
            );
        } finally {
            contextLogger.detachAppender(listAppender);
        }
    }

    @Test
    void shouldEmitShutdownLog() throws Exception {
        var triggerId = IdUtils.create();
        var topic = "tu_shutdown_" + IdUtils.create();

        var trigger = RealtimeTrigger.builder()
            .id(triggerId)
            .type(RealtimeTrigger.class.getName())
            .topic(topic)
            .groupId(Property.ofValue("test-group-" + IdUtils.create()))
            .properties(Property.ofValue(Map.of(
                "bootstrap.servers", this.bootstrap,
                "auto.offset.reset", "earliest"
            )))
            .serdeProperties(Property.ofValue(Map.of("schema.registry.url", this.registry)))
            .build();

        // stop() uses LoggerFactory.getLogger(RealtimeTrigger.class) — set level and attach appender there
        var triggerLogger = (Logger) LoggerFactory.getLogger(RealtimeTrigger.class);
        triggerLogger.setLevel(Level.INFO);
        var listAppender = new ListAppender<ILoggingEvent>();
        listAppender.setContext(triggerLogger.getLoggerContext());
        listAppender.start();
        triggerLogger.addAppender(listAppender);

        try {
            RunContext runContext = runContextFactory.of(Map.of());
            Consume task = trigger.consumeTask();

            Publisher<ConsumerRecord<Object, Object>> publisher = trigger.publisher(task, runContext);

            // Start on background thread, wait a bit for startup, then stop
            Flux.from(publisher)
                .subscribeOn(Schedulers.boundedElastic())
                .subscribe(record -> {}, err -> {});

            // Wait until the consumer is created (topic subscription blocks in poll)
            Thread.sleep(Duration.ofSeconds(4).toMillis());

            trigger.stop();

            // Allow wakeup to propagate and shutdown log to be written
            Thread.sleep(Duration.ofSeconds(2).toMillis());

            var logMessages = listAppender.list.stream()
                .map(ILoggingEvent::getFormattedMessage)
                .toList();

            assertThat(
                "Shutdown log must be emitted containing triggerId",
                logMessages.stream().anyMatch(m -> m.contains("Stopping Kafka trigger triggerId=") && m.contains(triggerId)),
                is(true)
            );
        } finally {
            triggerLogger.detachAppender(listAppender);
        }
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
