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
import org.apache.kafka.clients.admin.UserScramCredentialDeletion;

import java.util.List;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Delete a Kafka SCRAM user credential",
    description = "Removes a SASL/SCRAM credential from a user using the Kafka AdminClient. This operation is destructive and cannot be undone."
)
@Plugin(
    examples = {
        @Example(
            title = "Revoke a decommissioned tenant service account's credential",
            full = true,
            code = """
                id: kafka_scram_credential_delete
                namespace: company.team

                tasks:
                  - id: delete_credential
                    type: io.kestra.plugin.kafka.ScramCredentialDelete
                    properties:
                      bootstrap.servers: localhost:9092
                    user: tenant-acme-svc
                    mechanism: SCRAM_SHA_512
                """
        )
    }
)
public class ScramCredentialDelete extends AbstractKafkaAdminTask implements RunnableTask<ScramCredentialDelete.Output> {

    @Schema(title = "Username")
    @NotNull
    @PluginProperty(group = "main")
    private Property<String> user;

    @Schema(title = "SCRAM mechanism", description = "Defaults to `SCRAM_SHA_512`.")
    @NotNull
    @Builder.Default
    @PluginProperty(group = "main")
    private Property<ScramMechanism> mechanism = Property.ofValue(ScramMechanism.SCRAM_SHA_512);

    @Override
    public Output run(RunContext runContext) throws Exception {
        var rUser = requireRendered(runContext, this.user, String.class, "user");
        var rMechanism = runContext.render(this.mechanism).as(ScramMechanism.class).orElse(ScramMechanism.SCRAM_SHA_512);
        var timeout = renderTimeout(runContext);

        var deletion = new UserScramCredentialDeletion(rUser, rMechanism.toKafkaType());

        try (AdminClient admin = AdminClient.create(createAdminProperties(runContext))) {
            get(admin.alterUserScramCredentials(List.of(deletion)).all(), timeout);
        }

        return Output.builder().user(rUser).mechanism(rMechanism).build();
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(title = "Username")
        private final String user;

        @Schema(title = "SCRAM mechanism")
        private final ScramMechanism mechanism;
    }
}
