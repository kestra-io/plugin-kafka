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

import java.util.List;
import java.util.Map;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "List Kafka ACLs matching a filter",
    description = "Lists every ACL matching the given filter using the Kafka AdminClient. Unset filter fields match any value; an empty filter lists every ACL on the cluster."
)
@Plugin(
    examples = {
        @Example(
            title = "List every ACL granted on a tenant's topic namespace",
            full = true,
            code = """
                id: kafka_acl_list
                namespace: company.team

                tasks:
                  - id: list_acls
                    type: io.kestra.plugin.kafka.AclList
                    properties:
                      bootstrap.servers: localhost:9092
                    resourceType: TOPIC
                    resourceName: tenant_acme_
                    patternType: PREFIXED
                """
        )
    }
)
public class AclList extends AbstractAclFilterTask implements RunnableTask<AclList.Output> {

    @Override
    public Output run(RunContext runContext) throws Exception {
        var filter = buildFilter(runContext);
        var timeout = renderTimeout(runContext);

        try (AdminClient admin = AdminClient.create(createAdminProperties(runContext))) {
            var bindings = get(admin.describeAcls(filter).values(), timeout);
            return Output.builder()
                .acls(bindings.stream().map(AbstractAclFilterTask::toMap).toList())
                .build();
        }
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(title = "Matching ACLs", description = "Each entry contains `resourceType`, `resourceName`, `patternType`, `principal`, `host`, `operation` and `permissionType`.")
        private final List<Map<String, Object>> acls;
    }
}
