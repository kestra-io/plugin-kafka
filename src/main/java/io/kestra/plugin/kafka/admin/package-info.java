@PluginSubGroup(
    description = "This sub-group of plugins contains control-plane tasks for administering Apache Kafka clusters (topics, ACLs, quotas, SCRAM credentials, consumer groups) via the Kafka AdminClient, " +
        "typically used to provision and manage multi-tenant clusters. See https://kafka.apache.org/documentation/#operations_multitenancy.",
    categories = {
        PluginSubGroup.PluginCategory.DATA,
        PluginSubGroup.PluginCategory.INFRASTRUCTURE
    }
)
package io.kestra.plugin.kafka.admin;

import io.kestra.core.models.annotations.PluginSubGroup;
