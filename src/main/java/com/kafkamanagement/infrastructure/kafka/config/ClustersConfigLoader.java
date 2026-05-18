package com.kafkamanagement.infrastructure.kafka.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import lombok.Data;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.core.io.ResourceLoader;
import org.springframework.stereotype.Component;

import jakarta.annotation.PostConstruct;
import java.io.InputStream;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

@Slf4j
@Component
@RequiredArgsConstructor
public class ClustersConfigLoader {

    private final KafkaManagementProperties properties;
    private final ResourceLoader resourceLoader;
    private final Map<String, ClusterConfig> clusters = new ConcurrentHashMap<>();

    @PostConstruct
    public void loadClusters() {
        try {
            String configFile = properties.getClustersConfigFile();
            if (configFile == null || configFile.isBlank()) {
                log.warn("No clusters config file specified");
                return;
            }

            log.info("Loading clusters configuration from: {}", configFile);
            
            InputStream inputStream = resourceLoader.getResource(configFile).getInputStream();
            ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
            ClustersWrapper wrapper = mapper.readValue(inputStream, ClustersWrapper.class);

            if (wrapper.getClusters() != null) {
                for (ClusterConfig cluster : wrapper.getClusters()) {
                    clusters.put(cluster.getId(), cluster);
                    log.info("Loaded cluster: {} ({})", cluster.getName(), cluster.getId());
                }
            }

            log.info("Loaded {} clusters", clusters.size());

        } catch (Exception e) {
            log.error("Failed to load clusters configuration", e);
        }
    }

    public List<ClusterConfig> getAllClusters() {
        return List.copyOf(clusters.values());
    }

    public Optional<ClusterConfig> getCluster(String clusterId) {
        return Optional.ofNullable(clusters.get(clusterId));
    }

    public boolean clusterExists(String clusterId) {
        return clusters.containsKey(clusterId);
    }

    public List<String> getClusterIds() {
        return List.copyOf(clusters.keySet());
    }

    @Data
    private static class ClustersWrapper {
        private List<ClusterConfig> clusters = Collections.emptyList();
    }
}
