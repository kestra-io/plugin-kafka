package io.kestra.plugin.kafka;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.experimental.SuperBuilder;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.common.acl.AclBindingFilter;

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
        Unset filter fields match any value, so a filter with nothing set would match every ACL on the cluster — this task refuses to run in that case unless `deleteAll` is set to `true`.
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
                    type: io.kestra.plugin.kafka.AclDelete
                    properties:
                      bootstrap.servers: localhost:9092
                    principal: "User:tenant-acme-svc"
                """
        )
    }
)
public class AclDelete extends AbstractAclFilterTask implements RunnableTask<AclDelete.Output> {

    @Schema(
        title = "Confirm deleting every ACL on the cluster",
        description = "Required opt-in when every filter field is unset (or renders empty), which would otherwise match — and delete — every ACL on the cluster. Defaults to `false`."
    )
    @NotNull
    @Builder.Default
    @PluginProperty(group = "reliability")
    private Property<Boolean> deleteAll = Property.ofValue(false);

    @Override
    public Output run(RunContext runContext) throws Exception {
        var filter = buildFilter(runContext);
        var rDeleteAll = runContext.render(this.deleteAll).as(Boolean.class).orElse(false);

        if (!rDeleteAll && filter.equals(AclBindingFilter.ANY)) {
            throw new IllegalArgumentException(
                "No filter field is set (or all render empty) — this would delete every ACL on the cluster. " +
                    "Set at least one of 'resourceType', 'resourceName', 'patternType', 'principal', 'host', 'operation' or 'permissionType', " +
                    "or set 'deleteAll' to true to confirm this is intentional"
            );
        }

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
