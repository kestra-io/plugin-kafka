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
import org.apache.kafka.common.quota.ClientQuotaEntity;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Shared client quota entity fields for {@link QuotaAlter} and {@link QuotaDescribe}. A Kafka quota
 * entity is a combination of {@code user}, {@code client-id} and/or {@code ip}.
 */
@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
public abstract class AbstractQuotaEntityTask extends AbstractKafkaAdminTask {

    @Schema(title = "User entity name")
    @PluginProperty(group = "main")
    protected Property<String> entityUser;

    @Schema(title = "Client ID entity name")
    @PluginProperty(group = "main")
    protected Property<String> entityClientId;

    @Schema(title = "IP entity name")
    @PluginProperty(group = "main")
    protected Property<String> entityIp;

    protected Map<String, String> renderEntity(RunContext runContext) throws IllegalVariableEvaluationException {
        var entries = new LinkedHashMap<String, String>();
        runContext.render(this.entityUser).as(String.class).ifPresent(v -> entries.put(ClientQuotaEntity.USER, v));
        runContext.render(this.entityClientId).as(String.class).ifPresent(v -> entries.put(ClientQuotaEntity.CLIENT_ID, v));
        runContext.render(this.entityIp).as(String.class).ifPresent(v -> entries.put(ClientQuotaEntity.IP, v));
        return entries;
    }
}
