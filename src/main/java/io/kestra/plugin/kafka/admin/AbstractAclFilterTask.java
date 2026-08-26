package io.kestra.plugin.kafka.admin;

import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.experimental.SuperBuilder;
import org.apache.kafka.common.acl.AccessControlEntryFilter;
import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.acl.AclBindingFilter;
import org.apache.kafka.common.resource.ResourcePatternFilter;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Shared ACL filter fields for {@link AclList} and {@link AclDelete}: every field is optional and,
 * when unset, matches any value for that dimension (Kafka's {@code ANY} sentinel).
 */
@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
public abstract class AbstractAclFilterTask extends AbstractKafkaAdminTask {

    @Schema(title = "Resource type filter", description = "Matches any resource type when unset.")
    @PluginProperty(group = "processing")
    protected Property<ResourceType> resourceType;

    @Schema(title = "Resource name filter", description = "Matches any resource name when unset.")
    @PluginProperty(group = "processing")
    protected Property<String> resourceName;

    @Schema(title = "Resource pattern type filter", description = "Matches any pattern type when unset.")
    @PluginProperty(group = "processing")
    protected Property<PatternType> patternType;

    @Schema(title = "Principal filter", description = "For example `User:alice`. Matches any principal when unset.")
    @PluginProperty(group = "processing")
    protected Property<String> principal;

    @Schema(title = "Host filter", description = "Matches any host when unset.")
    @PluginProperty(group = "processing")
    protected Property<String> host;

    @Schema(title = "Operation filter", description = "Matches any operation when unset.")
    @PluginProperty(group = "processing")
    protected Property<AclOperation> operation;

    @Schema(title = "Permission type filter", description = "Matches any permission type when unset.")
    @PluginProperty(group = "processing")
    protected Property<AclPermissionType> permissionType;

    protected AclBindingFilter buildFilter(RunContext runContext) throws IllegalVariableEvaluationException {
        var rResourceType = runContext.render(this.resourceType).as(ResourceType.class)
            .map(ResourceType::toKafkaType)
            .orElse(org.apache.kafka.common.resource.ResourceType.ANY);
        var rResourceName = runContext.render(this.resourceName).as(String.class).orElse(null);
        var rPatternType = runContext.render(this.patternType).as(PatternType.class)
            .map(PatternType::toKafkaType)
            .orElse(org.apache.kafka.common.resource.PatternType.ANY);
        var rPrincipal = runContext.render(this.principal).as(String.class).orElse(null);
        var rHost = runContext.render(this.host).as(String.class).orElse(null);
        var rOperation = runContext.render(this.operation).as(AclOperation.class)
            .map(AclOperation::toKafkaType)
            .orElse(org.apache.kafka.common.acl.AclOperation.ANY);
        var rPermissionType = runContext.render(this.permissionType).as(AclPermissionType.class)
            .map(AclPermissionType::toKafkaType)
            .orElse(org.apache.kafka.common.acl.AclPermissionType.ANY);

        return new AclBindingFilter(
            new ResourcePatternFilter(rResourceType, rResourceName, rPatternType),
            new AccessControlEntryFilter(rPrincipal, rHost, rOperation, rPermissionType)
        );
    }

    protected static Map<String, Object> toMap(AclBinding binding) {
        var map = new LinkedHashMap<String, Object>();
        map.put("resourceType", binding.pattern().resourceType().name());
        map.put("resourceName", binding.pattern().name());
        map.put("patternType", binding.pattern().patternType().name());
        map.put("principal", binding.entry().principal());
        map.put("host", binding.entry().host());
        map.put("operation", binding.entry().operation().name());
        map.put("permissionType", binding.entry().permissionType().name());
        return map;
    }
}
