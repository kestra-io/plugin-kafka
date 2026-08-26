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
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.assertThrows;

@KestraTest
class AclAdminTest {

    @Inject
    private RunContextFactory runContextFactory;

    @Value("${kafka.bootstrap}")
    private String bootstrap;

    private Property<Map<String, String>> connection() {
        return Property.ofValue(Map.of("bootstrap.servers", this.bootstrap));
    }

    @Test
    void createListAndDeleteAcl() throws Exception {
        RunContext runContext = runContextFactory.of(Map.of());
        String resourceName = "tu_admin_acl_" + IdUtils.create();
        String principal = "User:" + IdUtils.create();

        AclCreate create = AclCreate.builder()
            .properties(connection())
            .resourceType(Property.ofValue(ResourceType.TOPIC))
            .resourceName(Property.ofValue(resourceName))
            .patternType(Property.ofValue(PatternType.PREFIXED))
            .principal(Property.ofValue(principal))
            .host(Property.ofValue("*"))
            .operation(Property.ofValue(AclOperation.WRITE))
            .permissionType(Property.ofValue(AclPermissionType.ALLOW))
            .build();

        AclCreate.Output createOutput = create.run(runContext);
        assertThat(createOutput.getResourceName(), is(resourceName));
        assertThat(createOutput.getPatternType(), is(PatternType.PREFIXED));

        AclList list = AclList.builder()
            .properties(connection())
            .principal(Property.ofValue(principal))
            .build();
        AclList.Output listOutput = list.run(runContext);
        assertThat(listOutput.getAcls(), hasSize(1));
        assertThat(listOutput.getAcls().getFirst().get("resourceName"), is(resourceName));

        AclDelete delete = AclDelete.builder()
            .properties(connection())
            .principal(Property.ofValue(principal))
            .build();
        AclDelete.Output deleteOutput = delete.run(runContext);
        assertThat(deleteOutput.getDeletedAcls(), hasSize(1));

        AclList afterDelete = AclList.builder()
            .properties(connection())
            .principal(Property.ofValue(principal))
            .build();
        assertThat(afterDelete.run(runContext).getAcls(), empty());
    }

    @Test
    void shouldThrowWhenRequiredFieldMissing() {
        RunContext runContext = runContextFactory.of(Map.of());

        AclCreate create = AclCreate.builder()
            .properties(connection())
            .resourceType(Property.ofValue(ResourceType.TOPIC))
            .resourceName(Property.ofValue("some_topic"))
            .operation(Property.ofValue(AclOperation.READ))
            .permissionType(Property.ofValue(AclPermissionType.ALLOW))
            .build();

        assertThrows(IllegalArgumentException.class, () -> create.run(runContext));
    }
}
