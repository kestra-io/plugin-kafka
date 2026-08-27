package io.kestra.plugin.kafka;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
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
import org.apache.kafka.common.quota.ClientQuotaFilter;
import org.apache.kafka.common.quota.ClientQuotaFilterComponent;

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
    title = "Describe Kafka client quotas",
    description = """
        Lists client quotas (`producer_byte_rate`, `consumer_byte_rate`, `request_percentage`, `controller_mutation_rate`) matching an entity filter using the Kafka AdminClient.
        Describes every quota on the cluster when no entity filter is set.
        """
)
@Plugin(
    examples = {
        @Example(
            title = "Inspect the quotas applied to a tenant service account",
            full = true,
            code = """
                id: kafka_quota_describe
                namespace: company.team

                tasks:
                  - id: describe_quota
                    type: io.kestra.plugin.kafka.QuotaDescribe
                    properties:
                      bootstrap.servers: localhost:9092
                    entityUser: tenant-acme-svc
                """
        )
    }
)
public class QuotaDescribe extends AbstractQuotaEntityTask implements RunnableTask<QuotaDescribe.Output> {

    @Override
    public Output run(RunContext runContext) throws Exception {
        var rEntity = renderEntity(runContext);
        var timeout = renderTimeout(runContext);

        ClientQuotaFilter filter;
        if (rEntity.isEmpty()) {
            filter = ClientQuotaFilter.all();
        } else {
            List<ClientQuotaFilterComponent> components = new ArrayList<>();
            rEntity.forEach((type, value) -> components.add(ClientQuotaFilterComponent.ofEntity(type, value)));
            filter = ClientQuotaFilter.contains(components);
        }

        try (AdminClient admin = AdminClient.create(createAdminProperties(runContext))) {
            var entities = get(admin.describeClientQuotas(filter).entities(), timeout);

            var quotas = entities.entrySet().stream()
                .map(entry -> {
                    var item = new LinkedHashMap<String, Object>();
                    item.put("entity", entry.getKey().entries());
                    item.put("quotas", entry.getValue());
                    return (Map<String, Object>) item;
                })
                .toList();

            return Output.builder().quotas(quotas).build();
        }
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(title = "Matching quota entities", description = "Each entry contains `entity` (keys among `user`, `client-id`, `ip`) and `quotas` (keys among `producer_byte_rate`, `consumer_byte_rate`, `request_percentage`, `controller_mutation_rate`).")
        private final List<Map<String, Object>> quotas;
    }
}
