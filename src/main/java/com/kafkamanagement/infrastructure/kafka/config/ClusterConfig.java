package com.kafkamanagement.infrastructure.kafka.config;

import lombok.Data;
import java.util.List;
import java.util.Map;

@Data
public class ClusterConfig {
    private String id;
    private String name;
    private String bootstrapServers;
    private Map<String, String> properties;
    private SchemaRegistryConfig schemaRegistry;
    private List<ConnectConfig> connect;

    @Data
    public static class SchemaRegistryConfig {
        private String url;
        private String basicAuthUsername;
        private String basicAuthPassword;
        private Map<String, String> properties;
    }

    @Data
    public static class ConnectConfig {
        private String name;
        private String url;
        private String basicAuthUsername;
        private String basicAuthPassword;
    }

    // Helper methods for Schema Registry
    public String getSchemaRegistryUrl() {
        return schemaRegistry != null ? schemaRegistry.getUrl() : null;
    }

    public String getSchemaRegistryUsername() {
        return schemaRegistry != null ? schemaRegistry.getBasicAuthUsername() : null;
    }

    public String getSchemaRegistryPassword() {
        return schemaRegistry != null ? schemaRegistry.getBasicAuthPassword() : null;
    }

    // Helper methods for Kafka Connect
    public ConnectConfig getConnectConfig(String connectName) {
        if (connect == null) return null;
        return connect.stream()
                .filter(c -> c.getName().equals(connectName))
                .findFirst()
                .orElse(null);
    }

    public String getDefaultConnectUrl() {
        if (connect == null || connect.isEmpty()) return null;
        return connect.get(0).getUrl();
    }
}
