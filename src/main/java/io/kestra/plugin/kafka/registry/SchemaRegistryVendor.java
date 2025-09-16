package io.kestra.plugin.kafka.registry;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.plugins.AdditionalPlugin;
import io.kestra.core.plugins.serdes.PluginDeserializer;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.kafka.serdes.SerdeType;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.experimental.SuperBuilder;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;

@Plugin
@SuperBuilder(toBuilder = true)
@Getter
@NoArgsConstructor
// IMPORTANT: The abstract plugin base class must define using the PluginDeserializer,
// AND concrete subclasses must be annotated by @JsonDeserialize() to avoid StackOverflow.
@JsonDeserialize(using = PluginDeserializer.class)
public abstract class SchemaRegistryVendor extends AdditionalPlugin {

    public abstract Serializer<?> getSerializer(RunContext runContext, SerdeType valueSerdeType) throws IllegalVariableEvaluationException;

    public abstract Deserializer<?> getDeserializer(RunContext runContext, SerdeType valueSerdeType) throws IllegalVariableEvaluationException;
}
