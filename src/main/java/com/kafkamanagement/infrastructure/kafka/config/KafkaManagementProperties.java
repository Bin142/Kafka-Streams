package com.kafkamanagement.infrastructure.kafka.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Data
@Component
@ConfigurationProperties(prefix = "kafka-management")
public class KafkaManagementProperties {

    private String clustersConfigFile;
    private DefaultsConfig defaults = new DefaultsConfig();
    private List<String> internalTopicsRegex = new ArrayList<>();
    private List<String> streamTopicsRegex = new ArrayList<>();
    private PaginationConfig pagination = new PaginationConfig();
    private MessageBrowserConfig messageBrowser = new MessageBrowserConfig();

    @Data
    public static class DefaultsConfig {
        private Map<String, String> admin = new HashMap<>();
        private Map<String, String> consumer = new HashMap<>();
        private Map<String, String> producer = new HashMap<>();
    }

    @Data
    public static class PaginationConfig {
        private int defaultPageSize = 25;
        private int maxPageSize = 100;
    }

    @Data
    public static class MessageBrowserConfig {
        private int defaultSize = 50;
        private int maxSize = 500;
        private int pollTimeoutMs = 5000;
    }
}
