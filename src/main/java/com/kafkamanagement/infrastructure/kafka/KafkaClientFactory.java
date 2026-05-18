package com.kafkamanagement.infrastructure.kafka;

import com.kafkamanagement.common.exception.InvalidClusterException;
import com.kafkamanagement.infrastructure.kafka.config.ClusterConfig;
import com.kafkamanagement.infrastructure.kafka.config.ClustersConfigLoader;
import com.kafkamanagement.infrastructure.kafka.config.KafkaManagementProperties;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.stereotype.Component;

import jakarta.annotation.PreDestroy;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

@Slf4j
@Component
@RequiredArgsConstructor
public class KafkaClientFactory {

    private final ClustersConfigLoader clustersConfigLoader;
    private final KafkaManagementProperties kafkaProperties;

    private final Map<String, AdminClient> adminClients = new ConcurrentHashMap<>();
    private final Map<String, KafkaProducer<byte[], byte[]>> producers = new ConcurrentHashMap<>();

    /**
     * Get or create AdminClient for cluster (cached)
     */
    public AdminClient getAdminClient(String clusterId) {
        validateCluster(clusterId);
        return adminClients.computeIfAbsent(clusterId, id -> {
            Properties props = buildAdminProperties(clusterId);
            log.info("Creating AdminClient for cluster: {}", clusterId);
            return AdminClient.create(props);
        });
    }

    /**
     * Get or create Producer for cluster (cached)
     */
    public KafkaProducer<byte[], byte[]> getProducer(String clusterId) {
        validateCluster(clusterId);
        return producers.computeIfAbsent(clusterId, id -> {
            Properties props = buildProducerProperties(clusterId);
            log.info("Creating Producer for cluster: {}", clusterId);
            return new KafkaProducer<>(props, new ByteArraySerializer(), new ByteArraySerializer());
        });
    }

    /**
     * Create new Consumer (not cached - each request needs own consumer)
     */
    public <K, V> KafkaConsumer<K, V> createConsumer(String clusterId) {
        return createConsumer(clusterId, new Properties());
    }

    /**
     * Create new Consumer with custom properties
     */
    @SuppressWarnings("unchecked")
    public <K, V> KafkaConsumer<K, V> createConsumer(String clusterId, Properties customProps) {
        validateCluster(clusterId);
        Properties props = buildConsumerProperties(clusterId);
        props.putAll(customProps);
        
        // Ensure unique group.id if not specified
        if (!props.containsKey(ConsumerConfig.GROUP_ID_CONFIG)) {
            props.put(ConsumerConfig.GROUP_ID_CONFIG, "kafka-management-" + UUID.randomUUID());
        }
        
        // Default to String deserializers for message browsing
        if (!props.containsKey(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG)) {
            props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        }
        if (!props.containsKey(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG)) {
            props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        }
        
        return new KafkaConsumer<>(props);
    }

    /**
     * Create new Producer (not cached - for message production)
     */
    @SuppressWarnings("unchecked")
    public <K, V> KafkaProducer<K, V> createProducer(String clusterId) {
        return createProducer(clusterId, new Properties());
    }

    /**
     * Create new Producer with custom properties
     */
    @SuppressWarnings("unchecked")
    public <K, V> KafkaProducer<K, V> createProducer(String clusterId, Properties customProps) {
        validateCluster(clusterId);
        Properties props = buildProducerProperties(clusterId);
        props.putAll(customProps);
        
        // Default to String serializers for message production
        if (!props.containsKey(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG)) {
            props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        }
        if (!props.containsKey(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG)) {
            props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        }
        
        return new KafkaProducer<>(props);
    }

    /**
     * Get cluster configuration
     */
    public ClusterConfig getClusterConfig(String clusterId) {
        return clustersConfigLoader.getCluster(clusterId)
                .orElseThrow(() -> new InvalidClusterException(clusterId));
    }

    /**
     * Check if cluster exists
     */
    public boolean clusterExists(String clusterId) {
        return clustersConfigLoader.clusterExists(clusterId);
    }

    private void validateCluster(String clusterId) {
        if (!clusterExists(clusterId)) {
            throw new InvalidClusterException(clusterId);
        }
    }

    private Properties buildAdminProperties(String clusterId) {
        Properties props = new Properties();
        
        // Add default admin properties
        props.putAll(kafkaProperties.getDefaults().getAdmin());
        
        // Add cluster-specific properties
        ClusterConfig cluster = getClusterConfig(clusterId);
        props.put("bootstrap.servers", cluster.getBootstrapServers());
        
        if (cluster.getProperties() != null) {
            props.putAll(cluster.getProperties());
        }
        
        return props;
    }

    private Properties buildConsumerProperties(String clusterId) {
        Properties props = new Properties();
        
        // Add default consumer properties
        props.putAll(kafkaProperties.getDefaults().getConsumer());
        
        // Add cluster-specific properties
        ClusterConfig cluster = getClusterConfig(clusterId);
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, cluster.getBootstrapServers());
        
        if (cluster.getProperties() != null) {
            props.putAll(cluster.getProperties());
        }
        
        return props;
    }

    private Properties buildProducerProperties(String clusterId) {
        Properties props = new Properties();
        
        // Add default producer properties
        props.putAll(kafkaProperties.getDefaults().getProducer());
        
        // Add cluster-specific properties
        ClusterConfig cluster = getClusterConfig(clusterId);
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, cluster.getBootstrapServers());
        
        if (cluster.getProperties() != null) {
            props.putAll(cluster.getProperties());
        }
        
        return props;
    }

    /**
     * Close all clients for a cluster
     */
    public void closeClients(String clusterId) {
        AdminClient adminClient = adminClients.remove(clusterId);
        if (adminClient != null) {
            log.info("Closing AdminClient for cluster: {}", clusterId);
            adminClient.close();
        }

        KafkaProducer<byte[], byte[]> producer = producers.remove(clusterId);
        if (producer != null) {
            log.info("Closing Producer for cluster: {}", clusterId);
            producer.close();
        }
    }

    /**
     * Close all clients (shutdown)
     */
    @PreDestroy
    public void closeAll() {
        log.info("Closing all Kafka clients...");
        
        adminClients.forEach((clusterId, client) -> {
            try {
                client.close();
                log.info("Closed AdminClient for cluster: {}", clusterId);
            } catch (Exception e) {
                log.error("Error closing AdminClient for cluster: {}", clusterId, e);
            }
        });
        adminClients.clear();

        producers.forEach((clusterId, producer) -> {
            try {
                producer.close();
                log.info("Closed Producer for cluster: {}", clusterId);
            } catch (Exception e) {
                log.error("Error closing Producer for cluster: {}", clusterId, e);
            }
        });
        producers.clear();
        
        log.info("All Kafka clients closed");
    }
}
