package com.kafkamanagement.infrastructure.schemaregistry;

import com.kafkamanagement.common.exception.BusinessException;
import com.kafkamanagement.common.exception.InvalidClusterException;
import com.kafkamanagement.infrastructure.kafka.config.ClusterConfig;
import com.kafkamanagement.infrastructure.kafka.config.ClustersConfigLoader;
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import jakarta.annotation.PreDestroy;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Slf4j
@Component
@RequiredArgsConstructor
public class SchemaRegistryClientFactory {

    private final ClustersConfigLoader clustersConfigLoader;
    
    private final Map<String, SchemaRegistryClient> clients = new ConcurrentHashMap<>();
    
    private static final int SCHEMA_CACHE_SIZE = 1000;

    /**
     * Get or create Schema Registry client for cluster
     */
    public SchemaRegistryClient getClient(String clusterId) {
        ClusterConfig cluster = clustersConfigLoader.getCluster(clusterId)
                .orElseThrow(() -> new InvalidClusterException(clusterId));
        
        if (cluster.getSchemaRegistryUrl() == null || cluster.getSchemaRegistryUrl().isBlank()) {
            throw new BusinessException("Schema Registry is not configured for cluster: " + clusterId);
        }
        
        return clients.computeIfAbsent(clusterId, id -> {
            log.info("Creating Schema Registry client for cluster: {}", clusterId);
            
            Map<String, Object> configs = new HashMap<>();
            
            // Add authentication if configured
            if (cluster.getSchemaRegistryUsername() != null && cluster.getSchemaRegistryPassword() != null) {
                configs.put("basic.auth.credentials.source", "USER_INFO");
                configs.put("basic.auth.user.info", 
                        cluster.getSchemaRegistryUsername() + ":" + cluster.getSchemaRegistryPassword());
            }
            
            return new CachedSchemaRegistryClient(
                    cluster.getSchemaRegistryUrl(),
                    SCHEMA_CACHE_SIZE,
                    configs
            );
        });
    }

    /**
     * Check if Schema Registry is configured for cluster
     */
    public boolean isSchemaRegistryConfigured(String clusterId) {
        return clustersConfigLoader.getCluster(clusterId)
                .map(cluster -> cluster.getSchemaRegistryUrl() != null && !cluster.getSchemaRegistryUrl().isBlank())
                .orElse(false);
    }

    /**
     * Close client for a cluster
     */
    public void closeClient(String clusterId) {
        SchemaRegistryClient client = clients.remove(clusterId);
        if (client != null) {
            try {
                client.close();
                log.info("Closed Schema Registry client for cluster: {}", clusterId);
            } catch (Exception e) {
                log.error("Error closing Schema Registry client for cluster: {}", clusterId, e);
            }
        }
    }

    /**
     * Close all clients
     */
    @PreDestroy
    public void closeAll() {
        log.info("Closing all Schema Registry clients...");
        clients.forEach((clusterId, client) -> {
            try {
                client.close();
                log.info("Closed Schema Registry client for cluster: {}", clusterId);
            } catch (Exception e) {
                log.error("Error closing Schema Registry client for cluster: {}", clusterId, e);
            }
        });
        clients.clear();
    }
}
