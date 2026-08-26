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
import org.apache.kafka.common.acl.AccessControlEntry;
import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.resource.ResourcePattern;

import java.util.List;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Create a Kafka ACL",
    description = """
        Grants (or denies) an operation on a resource to a principal using the Kafka AdminClient.
        Use `patternType: PREFIXED` to authorize an entire per-tenant namespace (for example all topics starting with `tenant_acme_`) with a single ACL.
        """
)
@Plugin(
    examples = {
        @Example(
            title = "Authorize a tenant service account to produce to its own topic namespace",
            full = true,
            code = """
                id: kafka_acl_create
                namespace: company.team

                tasks:
                  - id: create_acl
                    type: io.kestra.plugin.kafka.AclCreate
                    properties:
                      bootstrap.servers: localhost:9092
                    resourceType: TOPIC
                    resourceName: tenant_acme_
                    patternType: PREFIXED
                    principal: "User:tenant-acme-svc"
                    host: "*"
                    operation: WRITE
                    permissionType: ALLOW
                """
        )
    }
)
public class AclCreate extends AbstractKafkaAdminTask implements RunnableTask<AclCreate.Output> {

    @Schema(title = "Resource type", description = "For example `TOPIC`, `GROUP`, `CLUSTER`, `TRANSACTIONAL_ID`, `DELEGATION_TOKEN`, `USER`.")
    @NotNull
    @PluginProperty(group = "main")
    private Property<ResourceType> resourceType;

    @Schema(title = "Resource name", description = "Exact name for `LITERAL`, or the namespace prefix for `PREFIXED`.")
    @NotNull
    @PluginProperty(group = "main")
    private Property<String> resourceName;

    @Schema(
        title = "Resource pattern type",
        description = "`LITERAL` matches the resource name exactly (default). `PREFIXED` matches every resource whose name starts with `resourceName` — the standard way to authorize a whole per-tenant namespace with one ACL. `MATCH` matches wildcard and prefixed patterns as well as literal ones."
    )
    @NotNull
    @Builder.Default
    @PluginProperty(group = "main")
    private Property<PatternType> patternType = Property.ofValue(PatternType.LITERAL);

    @Schema(title = "Principal", description = "For example `User:alice`.")
    @NotNull
    @PluginProperty(group = "main")
    private Property<String> principal;

    @Schema(title = "Host", description = "Defaults to `*` (any host).")
    @NotNull
    @Builder.Default
    @PluginProperty(group = "main")
    private Property<String> host = Property.ofValue("*");

    @Schema(title = "Operation", description = "For example `READ`, `WRITE`, `CREATE`, `DELETE`, `ALTER`, `DESCRIBE`, `ALL`.")
    @NotNull
    @PluginProperty(group = "main")
    private Property<AclOperation> operation;

    @Schema(title = "Permission type", description = "`ALLOW` or `DENY`.")
    @NotNull
    @PluginProperty(group = "main")
    private Property<AclPermissionType> permissionType;

    @Override
    public Output run(RunContext runContext) throws Exception {
        var rResourceType = requireRendered(runContext, this.resourceType, ResourceType.class, "resourceType");
        var rResourceName = requireRendered(runContext, this.resourceName, String.class, "resourceName");
        var rPatternType = runContext.render(this.patternType).as(PatternType.class).orElse(PatternType.LITERAL);
        var rPrincipal = requireRendered(runContext, this.principal, String.class, "principal");
        var rHost = runContext.render(this.host).as(String.class).orElse("*");
        var rOperation = requireRendered(runContext, this.operation, AclOperation.class, "operation");
        var rPermissionType = requireRendered(runContext, this.permissionType, AclPermissionType.class, "permissionType");
        var timeout = renderTimeout(runContext);

        var binding = new AclBinding(
            new ResourcePattern(rResourceType.toKafkaType(), rResourceName, rPatternType.toKafkaType()),
            new AccessControlEntry(rPrincipal, rHost, rOperation.toKafkaType(), rPermissionType.toKafkaType())
        );

        try (AdminClient admin = AdminClient.create(createAdminProperties(runContext))) {
            get(admin.createAcls(List.of(binding)).all(), timeout);
        }

        return Output.builder()
            .resourceType(rResourceType)
            .resourceName(rResourceName)
            .patternType(rPatternType)
            .principal(rPrincipal)
            .host(rHost)
            .operation(rOperation)
            .permissionType(rPermissionType)
            .build();
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(title = "Resource type")
        private final ResourceType resourceType;

        @Schema(title = "Resource name")
        private final String resourceName;

        @Schema(title = "Resource pattern type")
        private final PatternType patternType;

        @Schema(title = "Principal")
        private final String principal;

        @Schema(title = "Host")
        private final String host;

        @Schema(title = "Operation")
        private final AclOperation operation;

        @Schema(title = "Permission type")
        private final AclPermissionType permissionType;
    }
}
