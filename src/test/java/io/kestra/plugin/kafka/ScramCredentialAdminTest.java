package io.kestra.plugin.kafka;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.utils.IdUtils;
import io.micronaut.context.annotation.Value;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

@KestraTest
class ScramCredentialAdminTest {

    @Inject
    private RunContextFactory runContextFactory;

    @Value("${kafka.bootstrap}")
    private String bootstrap;

    private Property<Map<String, String>> connection() {
        return Property.ofValue(Map.of("bootstrap.servers", this.bootstrap));
    }

    @Test
    void createScramCredentialWithOutOfRangeIterationsFails() {
        RunContext runContext = runContextFactory.of(Map.of());
        String user = "tu_admin_scram_" + IdUtils.create();

        ScramCredentialCreate create = ScramCredentialCreate.builder()
            .properties(connection())
            .user(Property.ofValue(user))
            .password(Property.ofValue("s3cr3t-password"))
            .mechanism(Property.ofValue(ScramMechanism.SCRAM_SHA_256))
            .iterations(Property.ofValue(1))
            .build();

        IllegalArgumentException exception = org.junit.jupiter.api.Assertions.assertThrows(
            IllegalArgumentException.class,
            () -> create.run(runContext)
        );
        assertThat(exception.getMessage(), org.hamcrest.Matchers.containsString("4096"));
        assertThat(exception.getMessage(), org.hamcrest.Matchers.containsString("16384"));
    }

    @Test
    void createAndDeleteScramCredential() throws Exception {
        RunContext runContext = runContextFactory.of(Map.of());
        String user = "tu_admin_scram_" + IdUtils.create();

        ScramCredentialCreate create = ScramCredentialCreate.builder()
            .properties(connection())
            .user(Property.ofValue(user))
            .password(Property.ofValue("s3cr3t-password"))
            .mechanism(Property.ofValue(ScramMechanism.SCRAM_SHA_256))
            .iterations(Property.ofValue(4096))
            .build();

        ScramCredentialCreate.Output createOutput = create.run(runContext);
        assertThat(createOutput.getUser(), is(user));
        assertThat(createOutput.getMechanism(), is(ScramMechanism.SCRAM_SHA_256));
        assertThat(createOutput.getIterations(), is(4096));

        ScramCredentialDelete delete = ScramCredentialDelete.builder()
            .properties(connection())
            .user(Property.ofValue(user))
            .mechanism(Property.ofValue(ScramMechanism.SCRAM_SHA_256))
            .build();

        ScramCredentialDelete.Output deleteOutput = delete.run(runContext);
        assertThat(deleteOutput.getUser(), is(user));
        assertThat(deleteOutput.getMechanism(), is(ScramMechanism.SCRAM_SHA_256));
    }
}
