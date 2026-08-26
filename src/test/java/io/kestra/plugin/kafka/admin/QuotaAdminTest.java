package io.kestra.plugin.kafka.admin;

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
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.assertThrows;

@KestraTest
class QuotaAdminTest {

    @Inject
    private RunContextFactory runContextFactory;

    @Value("${kafka.bootstrap}")
    private String bootstrap;

    private Property<Map<String, String>> connection() {
        return Property.ofValue(Map.of("bootstrap.servers", this.bootstrap));
    }

    @Test
    void alterAndDescribeQuota() throws Exception {
        RunContext runContext = runContextFactory.of(Map.of());
        String user = "tu_admin_quota_" + IdUtils.create();

        QuotaAlter alter = QuotaAlter.builder()
            .properties(connection())
            .entityUser(Property.ofValue(user))
            .producerByteRate(Property.ofValue(1048576D))
            .consumerByteRate(Property.ofValue(2097152D))
            .build();

        QuotaAlter.Output alterOutput = alter.run(runContext);
        assertThat(alterOutput.getEntity().get("user"), is(user));
        assertThat(alterOutput.getAppliedQuotas().get("producer_byte_rate"), is(1048576D));

        QuotaDescribe describe = QuotaDescribe.builder()
            .properties(connection())
            .entityUser(Property.ofValue(user))
            .build();

        QuotaDescribe.Output describeOutput = describe.run(runContext);
        assertThat(describeOutput.getQuotas(), hasSize(1));
        @SuppressWarnings("unchecked")
        Map<String, Object> entity = (Map<String, Object>) describeOutput.getQuotas().getFirst().get("entity");
        assertThat(entity.get("user"), is(user));
    }

    @Test
    void shouldThrowWhenNoEntityProvided() {
        RunContext runContext = runContextFactory.of(Map.of());

        QuotaAlter alter = QuotaAlter.builder()
            .properties(connection())
            .producerByteRate(Property.ofValue(1D))
            .build();

        assertThrows(IllegalArgumentException.class, () -> alter.run(runContext));
    }

    @Test
    void shouldThrowWhenNoQuotaProvided() {
        RunContext runContext = runContextFactory.of(Map.of());

        QuotaAlter alter = QuotaAlter.builder()
            .properties(connection())
            .entityUser(Property.ofValue("some-user"))
            .build();

        assertThrows(IllegalArgumentException.class, () -> alter.run(runContext));
    }
}
