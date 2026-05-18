package com.kafkamanagement.application.connector.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class ConnectorPluginDTO {
    private String className;
    private String type; // source, sink, converter, header_converter, predicate, transformation
    private String version;
    private List<ConfigDefinition> configDefinitions;

    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    public static class ConfigDefinition {
        private String name;
        private String type;
        private boolean required;
        private String defaultValue;
        private String importance;
        private String documentation;
        private String group;
        private int orderInGroup;
        private String width;
        private String displayName;
        private List<String> dependents;
    }
}
