package io.kestra.plugin.kafka.admin;

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
    title = "Delete Kafka ACLs matching a filter",
    description = """
        Deletes every ACL matching the given filter using the Kafka AdminClient. This operation is destructive and cannot be undone.
        Unset filter fields match any value, so an empty filter deletes every ACL on the cluster — scope `resourceName`/`resourceType` carefully.
        """
)
@Plugin(
    examples = {
        @Example(
            title = "Revoke every ACL granted to a decommissioned tenant service account",
            full = true,
            code = """
                id: kafka_acl_delete
                namespace: company.team

                tasks:
                  - id: delete_acls
                    type: io.kestra.plugin.kafka.admin.AclDelete
                    properties:
                      bootstrap.servers: localhost:9092
                    principal: "User:tenant-acme-svc"
                """
        )
    }
)
public class AclDelete extends AbstractAclFilterTask implements RunnableTask<AclDelete.Output> {

    @Override
    public Output run(RunContext runContext) throws Exception {
        var filter = buildFilter(runContext);
        var timeout = renderTimeout(runContext);

        try (AdminClient admin = AdminClient.create(createAdminProperties(runContext))) {
            var deleted = get(admin.deleteAcls(List.of(filter)).all(), timeout);
            return Output.builder()
                .deletedAcls(deleted.stream().map(AbstractAclFilterTask::toMap).toList())
                .build();
        }
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(title = "Deleted ACLs", description = "Each entry contains `resourceType`, `resourceName`, `patternType`, `principal`, `host`, `operation` and `permissionType`.")
        private final List<Map<String, Object>> deletedAcls;
    }
}
