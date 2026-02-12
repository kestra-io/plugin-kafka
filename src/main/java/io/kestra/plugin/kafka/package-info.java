@PluginSubGroup(
    description = "This sub-group of plugins contains tasks for using the Apache Kafka message broker.\n" +
        "Apache Kafka is an open-source distributed event streaming platform used by thousands of companies for high-performance data pipelines, streaming analytics, data integration, and mission-critical applications.",categories = {
        PluginSubGroup.PluginCategory.DATA,
        PluginSubGroup.PluginCategory.INFRASTRUCTURE
    }
)
package io.kestra.plugin.kafka;

import io.kestra.core.models.annotations.PluginSubGroup;