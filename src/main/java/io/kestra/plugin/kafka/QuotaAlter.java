package io.kestra.plugin.kafka;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.experimental.SuperBuilder;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.common.quota.ClientQuotaAlteration;
import org.apache.kafka.common.quota.ClientQuotaEntity;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Alter a Kafka client quota",
    description = """
        Sets throughput and rate quotas for a `user`, `client-id`, `ip` entity (or a combination of them) using the Kafka AdminClient.
        Valid on an entity that doesn't have quotas set yet — Kafka creates them on first alteration. At least one quota value must be provided.
        """
)
@Plugin(
    examples = {
        @Example(
            title = "Cap a tenant's produce/consume throughput and controller mutation rate",
            full = true,
            code = """
                id: kafka_quota_alter
                namespace: company.team

                tasks:
                  - id: alter_quota
                    type: io.kestra.plugin.kafka.QuotaAlter
                    properties:
                      bootstrap.servers: localhost:9092
                    entityUser: tenant-acme-svc
                    producerByteRate: 1048576
                    consumerByteRate: 2097152
                    controllerMutationRate: 10
                """
        )
    }
)
public class QuotaAlter extends AbstractQuotaEntityTask implements RunnableTask<QuotaAlter.Output> {

    @Schema(title = "Producer byte rate", description = "Maps to Kafka's `producer_byte_rate` quota, in bytes/second.")
    @PluginProperty(group = "main")
    private Property<Double> producerByteRate;

    @Schema(title = "Consumer byte rate", description = "Maps to Kafka's `consumer_byte_rate` quota, in bytes/second.")
    @PluginProperty(group = "main")
    private Property<Double> consumerByteRate;

    @Schema(title = "Request percentage", description = "Maps to Kafka's `request_percentage` quota, as a percentage of request handler thread time.")
    @PluginProperty(group = "main")
    private Property<Double> requestPercentage;

    @Schema(title = "Controller mutation rate", description = "Maps to Kafka's `controller_mutation_rate` quota, in mutations/second.")
    @PluginProperty(group = "main")
    private Property<Double> controllerMutationRate;

    @Override
    public Output run(RunContext runContext) throws Exception {
        var rEntity = renderEntity(runContext);
        if (rEntity.isEmpty()) {
            throw new IllegalArgumentException("At least one of 'entityUser', 'entityClientId' or 'entityIp' must be set");
        }

        var rQuotas = new LinkedHashMap<String, Double>();
        runContext.render(this.producerByteRate).as(Double.class).ifPresent(v -> rQuotas.put("producer_byte_rate", v));
        runContext.render(this.consumerByteRate).as(Double.class).ifPresent(v -> rQuotas.put("consumer_byte_rate", v));
        runContext.render(this.requestPercentage).as(Double.class).ifPresent(v -> rQuotas.put("request_percentage", v));
        runContext.render(this.controllerMutationRate).as(Double.class).ifPresent(v -> rQuotas.put("controller_mutation_rate", v));

        if (rQuotas.isEmpty()) {
            throw new IllegalArgumentException("At least one of 'producerByteRate', 'consumerByteRate', 'requestPercentage' or 'controllerMutationRate' must be set");
        }

        var timeout = renderTimeout(runContext);

        List<ClientQuotaAlteration.Op> ops = new ArrayList<>();
        rQuotas.forEach((key, value) -> ops.add(new ClientQuotaAlteration.Op(key, value)));

        var alteration = new ClientQuotaAlteration(new ClientQuotaEntity(rEntity), ops);

        try (AdminClient admin = AdminClient.create(createAdminProperties(runContext))) {
            get(admin.alterClientQuotas(List.of(alteration)).all(), timeout);
        }

        return Output.builder().entity(rEntity).appliedQuotas(rQuotas).build();
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(title = "Quota entity", description = "Keys among `user`, `client-id`, `ip`.")
        private final Map<String, String> entity;

        @Schema(title = "Quotas that were set", description = "Keys among `producer_byte_rate`, `consumer_byte_rate`, `request_percentage`, `controller_mutation_rate`.")
        private final Map<String, Double> appliedQuotas;
    }
}
