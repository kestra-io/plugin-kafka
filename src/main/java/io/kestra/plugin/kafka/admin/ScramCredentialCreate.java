package io.kestra.plugin.kafka.admin;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotNull;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.experimental.SuperBuilder;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.ScramCredentialInfo;
import org.apache.kafka.clients.admin.UserScramCredentialUpsertion;

import java.util.List;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Create or update a Kafka SCRAM user credential",
    description = "Upserts a SASL/SCRAM credential for a user using the Kafka AdminClient, typically to provision a per-tenant service account."
)
@Plugin(
    examples = {
        @Example(
            title = "Provision a SCRAM credential for a tenant service account",
            full = true,
            code = """
                id: kafka_scram_credential_create
                namespace: company.team

                tasks:
                  - id: create_credential
                    type: io.kestra.plugin.kafka.admin.ScramCredentialCreate
                    properties:
                      bootstrap.servers: localhost:9092
                    user: tenant-acme-svc
                    password: "{{ secret('TENANT_ACME_KAFKA_PASSWORD') }}"
                    mechanism: SCRAM_SHA_512
                """
        )
    }
)
public class ScramCredentialCreate extends AbstractKafkaAdminTask implements RunnableTask<ScramCredentialCreate.Output> {

    @Schema(title = "Username")
    @NotNull
    @PluginProperty(group = "main")
    private Property<String> user;

    @Schema(title = "Password")
    @NotNull
    @ToString.Exclude
    @PluginProperty(group = "main", secret = true)
    private Property<String> password;

    @Schema(title = "SCRAM mechanism", description = "Defaults to `SCRAM_SHA_512`.")
    @NotNull
    @Builder.Default
    @PluginProperty(group = "main")
    private Property<ScramMechanism> mechanism = Property.ofValue(ScramMechanism.SCRAM_SHA_512);

    @Schema(
        title = "Iteration count",
        description = "Number of SCRAM hashing iterations, between 4096 and 16384. Defaults to 4096."
    )
    @NotNull
    @Min(4096)
    @Max(16384)
    @Builder.Default
    @PluginProperty(group = "advanced")
    private Property<Integer> iterations = Property.ofValue(4096);

    @Override
    public Output run(RunContext runContext) throws Exception {
        var rUser = runContext.render(this.user).as(String.class)
            .orElseThrow(() -> new IllegalArgumentException("Missing required property 'user'"));
        var rPassword = runContext.render(this.password).as(String.class)
            .orElseThrow(() -> new IllegalArgumentException("Missing required property 'password'"));
        var rMechanism = runContext.render(this.mechanism).as(ScramMechanism.class).orElse(ScramMechanism.SCRAM_SHA_512);
        var rIterations = runContext.render(this.iterations).as(Integer.class).orElse(4096);
        var timeout = renderTimeout(runContext);

        var upsertion = new UserScramCredentialUpsertion(
            rUser,
            new ScramCredentialInfo(rMechanism.toKafkaType(), rIterations),
            rPassword
        );

        try (AdminClient admin = AdminClient.create(createAdminProperties(runContext))) {
            get(admin.alterUserScramCredentials(List.of(upsertion)).all(), timeout);
        }

        return Output.builder().user(rUser).mechanism(rMechanism).iterations(rIterations).build();
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(title = "Username")
        private final String user;

        @Schema(title = "SCRAM mechanism")
        private final ScramMechanism mechanism;

        @Schema(title = "Iteration count")
        private final Integer iterations;
    }
}
