package com.kafkamanagement.infrastructure.kafka;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.acl.AclBindingFilter;
import org.apache.kafka.common.config.ConfigResource;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

@Slf4j
@Component
@RequiredArgsConstructor
public class KafkaAdminWrapper {

    private final KafkaClientFactory kafkaClientFactory;

    // ==================== Cluster Operations ====================

    public String getClusterId(String clusterId) throws ExecutionException, InterruptedException {
        return kafkaClientFactory.getAdminClient(clusterId)
                .describeCluster()
                .clusterId()
                .get();
    }

    public Collection<Node> getNodes(String clusterId) throws ExecutionException, InterruptedException {
        return kafkaClientFactory.getAdminClient(clusterId)
                .describeCluster()
                .nodes()
                .get();
    }

    public Node getController(String clusterId) throws ExecutionException, InterruptedException {
        return kafkaClientFactory.getAdminClient(clusterId)
                .describeCluster()
                .controller()
                .get();
    }

    // ==================== Topic Operations ====================

    public Collection<TopicListing> listTopics(String clusterId) throws ExecutionException, InterruptedException {
        return kafkaClientFactory.getAdminClient(clusterId)
                .listTopics(new ListTopicsOptions().listInternal(true))
                .listings()
                .get();
    }

    public Map<String, TopicDescription> describeTopics(String clusterId, List<String> topicNames) 
            throws ExecutionException, InterruptedException {
        return kafkaClientFactory.getAdminClient(clusterId)
                .describeTopics(topicNames)
                .allTopicNames()
                .get();
    }

    public void createTopic(String clusterId, String name, int partitions, short replicationFactor,
                            Map<String, String> configs) throws ExecutionException, InterruptedException {
        NewTopic newTopic = new NewTopic(name, partitions, replicationFactor);
        if (configs != null && !configs.isEmpty()) {
            newTopic.configs(configs);
        }
        
        kafkaClientFactory.getAdminClient(clusterId)
                .createTopics(Collections.singleton(newTopic))
                .all()
                .get();
        
        log.info("Created topic {} in cluster {}", name, clusterId);
    }

    public void deleteTopic(String clusterId, String topicName) throws ExecutionException, InterruptedException {
        kafkaClientFactory.getAdminClient(clusterId)
                .deleteTopics(Collections.singleton(topicName))
                .all()
                .get();
        
        log.info("Deleted topic {} from cluster {}", topicName, clusterId);
    }

    public void increasePartitions(String clusterId, String topicName, int newPartitionCount) 
            throws ExecutionException, InterruptedException {
        Map<String, NewPartitions> newPartitions = new HashMap<>();
        newPartitions.put(topicName, NewPartitions.increaseTo(newPartitionCount));
        
        kafkaClientFactory.getAdminClient(clusterId)
                .createPartitions(newPartitions)
                .all()
                .get();
        
        log.info("Increased partitions for topic {} to {} in cluster {}", topicName, newPartitionCount, clusterId);
    }

    // ==================== Topic Config Operations ====================

    public Map<String, String> getTopicConfigs(String clusterId, String topicName) 
            throws ExecutionException, InterruptedException {
        ConfigResource resource = new ConfigResource(ConfigResource.Type.TOPIC, topicName);
        
        Config config = kafkaClientFactory.getAdminClient(clusterId)
                .describeConfigs(Collections.singleton(resource))
                .all()
                .get()
                .get(resource);
        
        return config.entries().stream()
                .collect(Collectors.toMap(ConfigEntry::name, ConfigEntry::value));
    }

    public Collection<ConfigEntry> getTopicConfigEntries(String clusterId, String topicName) 
            throws ExecutionException, InterruptedException {
        ConfigResource resource = new ConfigResource(ConfigResource.Type.TOPIC, topicName);
        
        Config config = kafkaClientFactory.getAdminClient(clusterId)
                .describeConfigs(Collections.singleton(resource))
                .all()
                .get()
                .get(resource);
        
        return config.entries();
    }

    public void updateTopicConfigs(String clusterId, String topicName, Map<String, String> configs) 
            throws ExecutionException, InterruptedException {
        ConfigResource resource = new ConfigResource(ConfigResource.Type.TOPIC, topicName);
        
        Collection<AlterConfigOp> alterOps = configs.entrySet().stream()
                .map(entry -> new AlterConfigOp(
                        new ConfigEntry(entry.getKey(), entry.getValue()),
                        AlterConfigOp.OpType.SET
                ))
                .collect(Collectors.toList());
        
        Map<ConfigResource, Collection<AlterConfigOp>> alterConfigs = new HashMap<>();
        alterConfigs.put(resource, alterOps);
        
        kafkaClientFactory.getAdminClient(clusterId)
                .incrementalAlterConfigs(alterConfigs)
                .all()
                .get();
        
        log.info("Updated configs for topic {} in cluster {}", topicName, clusterId);
    }

    // ==================== Topic Offset Operations ====================

    public Map<TopicPartition, Long> getBeginningOffsets(String clusterId, List<TopicPartition> partitions) {
        try (KafkaConsumer<byte[], byte[]> consumer = kafkaClientFactory.createConsumer(clusterId)) {
            return consumer.beginningOffsets(partitions);
        }
    }

    public Map<TopicPartition, Long> getEndOffsets(String clusterId, List<TopicPartition> partitions) {
        try (KafkaConsumer<byte[], byte[]> consumer = kafkaClientFactory.createConsumer(clusterId)) {
            return consumer.endOffsets(partitions);
        }
    }

    public Map<TopicPartition, Long> getOffsetsForTimes(String clusterId, Map<TopicPartition, Long> timestampsToSearch) {
        try (KafkaConsumer<byte[], byte[]> consumer = kafkaClientFactory.createConsumer(clusterId)) {
            return consumer.offsetsForTimes(timestampsToSearch).entrySet().stream()
                    .filter(e -> e.getValue() != null)
                    .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().offset()));
        }
    }

    // ==================== Consumer Group Operations ====================

    public Collection<ConsumerGroupListing> listConsumerGroups(String clusterId) 
            throws ExecutionException, InterruptedException {
        return kafkaClientFactory.getAdminClient(clusterId)
                .listConsumerGroups()
                .all()
                .get();
    }

    public Map<String, ConsumerGroupDescription> describeConsumerGroups(String clusterId, List<String> groupIds) 
            throws ExecutionException, InterruptedException {
        return kafkaClientFactory.getAdminClient(clusterId)
                .describeConsumerGroups(groupIds)
                .all()
                .get();
    }

    public Map<TopicPartition, OffsetAndMetadata> getConsumerGroupOffsets(String clusterId, String groupId) 
            throws ExecutionException, InterruptedException {
        return kafkaClientFactory.getAdminClient(clusterId)
                .listConsumerGroupOffsets(groupId)
                .partitionsToOffsetAndMetadata()
                .get();
    }

    public void deleteConsumerGroup(String clusterId, String groupId) 
            throws ExecutionException, InterruptedException {
        kafkaClientFactory.getAdminClient(clusterId)
                .deleteConsumerGroups(Collections.singleton(groupId))
                .all()
                .get();
        
        log.info("Deleted consumer group {} from cluster {}", groupId, clusterId);
    }

    public void deleteConsumerGroupOffsets(String clusterId, String groupId, Set<TopicPartition> partitions) 
            throws ExecutionException, InterruptedException {
        kafkaClientFactory.getAdminClient(clusterId)
                .deleteConsumerGroupOffsets(groupId, partitions)
                .all()
                .get();
        
        log.info("Deleted offsets for consumer group {} in cluster {}", groupId, clusterId);
    }

    public void alterConsumerGroupOffsets(String clusterId, String groupId, 
                                          Map<TopicPartition, OffsetAndMetadata> offsets) 
            throws ExecutionException, InterruptedException {
        kafkaClientFactory.getAdminClient(clusterId)
                .alterConsumerGroupOffsets(groupId, offsets)
                .all()
                .get();
        
        log.info("Altered offsets for consumer group {} in cluster {}", groupId, clusterId);
    }

    // ==================== Broker Config Operations ====================

    public Collection<ConfigEntry> getBrokerConfigs(String clusterId, int brokerId) 
            throws ExecutionException, InterruptedException {
        ConfigResource resource = new ConfigResource(ConfigResource.Type.BROKER, String.valueOf(brokerId));
        
        Config config = kafkaClientFactory.getAdminClient(clusterId)
                .describeConfigs(Collections.singleton(resource))
                .all()
                .get()
                .get(resource);
        
        return config.entries();
    }

    // ==================== Log Dir Operations ====================

    public Map<Integer, Map<String, LogDirDescription>> describeLogDirs(String clusterId) 
            throws ExecutionException, InterruptedException {
        Collection<Node> nodes = getNodes(clusterId);
        List<Integer> brokerIds = nodes.stream().map(Node::id).collect(Collectors.toList());
        
        return kafkaClientFactory.getAdminClient(clusterId)
                .describeLogDirs(brokerIds)
                .allDescriptions()
                .get();
    }

    // ==================== ACL Operations ====================

    public Collection<AclBinding> describeAcls(String clusterId, AclBindingFilter filter) 
            throws ExecutionException, InterruptedException {
        return kafkaClientFactory.getAdminClient(clusterId)
                .describeAcls(filter)
                .values()
                .get();
    }

    public void createAcls(String clusterId, Collection<AclBinding> acls) 
            throws ExecutionException, InterruptedException {
        kafkaClientFactory.getAdminClient(clusterId)
                .createAcls(acls)
                .all()
                .get();
        
        log.info("Created {} ACLs in cluster {}", acls.size(), clusterId);
    }

    public void deleteAcls(String clusterId, Collection<AclBindingFilter> filters) 
            throws ExecutionException, InterruptedException {
        kafkaClientFactory.getAdminClient(clusterId)
                .deleteAcls(filters)
                .all()
                .get();
        
        log.info("Deleted ACLs in cluster {}", clusterId);
    }

    // ==================== Delete Records ====================

    public void deleteRecords(String clusterId, Map<TopicPartition, Long> offsets) 
            throws ExecutionException, InterruptedException {
        Map<TopicPartition, RecordsToDelete> recordsToDelete = offsets.entrySet().stream()
                .collect(Collectors.toMap(
                        Map.Entry::getKey,
                        e -> RecordsToDelete.beforeOffset(e.getValue())
                ));
        
        kafkaClientFactory.getAdminClient(clusterId)
                .deleteRecords(recordsToDelete)
                .all()
                .get();
        
        log.info("Deleted records in cluster {}", clusterId);
    }

    /**
     * Delete all records from a topic (empty the topic)
     */
    public void deleteAllRecords(String clusterId, String topicName) 
            throws ExecutionException, InterruptedException {
        List<TopicPartition> partitions = getTopicPartitions(clusterId, topicName);
        Map<TopicPartition, Long> endOffsets = getEndOffsets(clusterId, partitions);
        
        Map<TopicPartition, RecordsToDelete> recordsToDelete = endOffsets.entrySet().stream()
                .collect(Collectors.toMap(
                        Map.Entry::getKey,
                        e -> RecordsToDelete.beforeOffset(e.getValue())
                ));
        
        kafkaClientFactory.getAdminClient(clusterId)
                .deleteRecords(recordsToDelete)
                .all()
                .get();
        
        log.info("Deleted all records from topic {} in cluster {}", topicName, clusterId);
    }

    // ==================== Helper Methods ====================

    public List<TopicPartition> getTopicPartitions(String clusterId, String topicName) 
            throws ExecutionException, InterruptedException {
        TopicDescription description = describeTopics(clusterId, List.of(topicName)).get(topicName);
        return description.partitions().stream()
                .map(p -> new TopicPartition(topicName, p.partition()))
                .collect(Collectors.toList());
    }
}
