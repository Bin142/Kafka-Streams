package com.kafkamanagement.application.topic;

import com.kafkamanagement.application.topic.dto.*;
import com.kafkamanagement.common.exception.BusinessException;
import com.kafkamanagement.common.exception.ResourceNotFoundException;
import com.kafkamanagement.common.security.PermissionChecker;
import com.kafkamanagement.common.security.RequirePermission;
import com.kafkamanagement.domain.user.model.Action;
import com.kafkamanagement.domain.user.model.Resource;
import com.kafkamanagement.infrastructure.kafka.KafkaAdminWrapper;
import com.kafkamanagement.infrastructure.kafka.config.KafkaManagementProperties;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.admin.TopicListing;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionInfo;
import org.springframework.cache.annotation.CacheEvict;
import org.springframework.cache.annotation.Cacheable;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageImpl;
import org.springframework.data.domain.Pageable;
import org.springframework.stereotype.Service;

import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

@Slf4j
@Service
@RequiredArgsConstructor
public class TopicService {

    private final KafkaAdminWrapper kafkaAdminWrapper;
    private final KafkaManagementProperties properties;
    private final PermissionChecker permissionChecker;

    /**
     * List topics with filtering and pagination
     */
    @Cacheable(value = "topics", key = "#clusterId + '-' + #pageable.pageNumber + '-' + #search + '-' + #view")
    public Page<TopicDTO> listTopics(String clusterId, Pageable pageable, String search, TopicListView view) {
        permissionChecker.checkPermission(clusterId, Resource.TOPIC, Action.READ);
        
        try {
            Collection<TopicListing> allTopics = kafkaAdminWrapper.listTopics(clusterId);
            
            // Filter topics
            List<String> filteredTopicNames = allTopics.stream()
                    .map(TopicListing::name)
                    .filter(name -> matchesSearch(name, search))
                    .filter(name -> matchesView(name, view))
                    .filter(name -> permissionChecker.hasPermission(clusterId, Resource.TOPIC, Action.READ, name))
                    .sorted(String::compareToIgnoreCase)
                    .collect(Collectors.toList());
            
            // Paginate
            int start = (int) pageable.getOffset();
            int end = Math.min(start + pageable.getPageSize(), filteredTopicNames.size());
            
            if (start > filteredTopicNames.size()) {
                return new PageImpl<>(Collections.emptyList(), pageable, filteredTopicNames.size());
            }
            
            List<String> pageTopicNames = filteredTopicNames.subList(start, end);
            
            // Get topic details
            Map<String, TopicDescription> descriptions = kafkaAdminWrapper.describeTopics(clusterId, pageTopicNames);
            
            // Get offsets for message count
            List<TopicPartition> partitions = descriptions.values().stream()
                    .flatMap(desc -> desc.partitions().stream()
                            .map(p -> new TopicPartition(desc.name(), p.partition())))
                    .collect(Collectors.toList());
            
            Map<TopicPartition, Long> beginOffsets = kafkaAdminWrapper.getBeginningOffsets(clusterId, partitions);
            Map<TopicPartition, Long> endOffsets = kafkaAdminWrapper.getEndOffsets(clusterId, partitions);
            
            List<TopicDTO> topics = descriptions.values().stream()
                    .map(desc -> {
                        long messageCount = calculateMessageCount(desc.name(), desc.partitions(), beginOffsets, endOffsets);
                        return TopicDTO.builder()
                                .name(desc.name())
                                .partitionCount(desc.partitions().size())
                                .replicationFactor(desc.partitions().isEmpty() ? 0 : 
                                        desc.partitions().get(0).replicas().size())
                                .internal(desc.isInternal())
                                .messageCount(messageCount)
                                .build();
                    })
                    .sorted(Comparator.comparing(TopicDTO::getName, String.CASE_INSENSITIVE_ORDER))
                    .collect(Collectors.toList());
            
            return new PageImpl<>(topics, pageable, filteredTopicNames.size());
            
        } catch (ExecutionException | InterruptedException e) {
            log.error("Failed to list topics for cluster {}", clusterId, e);
            throw new BusinessException("Failed to list topics: " + e.getMessage());
        }
    }

    /**
     * Get topic details
     */
    @Cacheable(value = "topic", key = "#clusterId + '-' + #topicName")
    public TopicDetailDTO getTopic(String clusterId, String topicName) {
        permissionChecker.checkPermission(clusterId, Resource.TOPIC, Action.READ, topicName);
        
        try {
            Map<String, TopicDescription> descriptions = kafkaAdminWrapper.describeTopics(clusterId, List.of(topicName));
            TopicDescription description = descriptions.get(topicName);
            
            if (description == null) {
                throw new ResourceNotFoundException("Topic", topicName);
            }
            
            // Get offsets
            List<TopicPartition> partitions = description.partitions().stream()
                    .map(p -> new TopicPartition(topicName, p.partition()))
                    .collect(Collectors.toList());
            
            Map<TopicPartition, Long> beginOffsets = kafkaAdminWrapper.getBeginningOffsets(clusterId, partitions);
            Map<TopicPartition, Long> endOffsets = kafkaAdminWrapper.getEndOffsets(clusterId, partitions);
            
            List<TopicDetailDTO.PartitionDTO> partitionDTOs = description.partitions().stream()
                    .map(p -> {
                        TopicPartition tp = new TopicPartition(topicName, p.partition());
                        long begin = beginOffsets.getOrDefault(tp, 0L);
                        long end = endOffsets.getOrDefault(tp, 0L);
                        
                        return TopicDetailDTO.PartitionDTO.builder()
                                .partition(p.partition())
                                .leader(p.leader() != null ? p.leader().id() : -1)
                                .replicas(p.replicas().stream().map(r -> r.id()).collect(Collectors.toList()))
                                .isr(p.isr().stream().map(r -> r.id()).collect(Collectors.toList()))
                                .beginningOffset(begin)
                                .endOffset(end)
                                .messageCount(end - begin)
                                .build();
                    })
                    .sorted(Comparator.comparingInt(TopicDetailDTO.PartitionDTO::getPartition))
                    .collect(Collectors.toList());
            
            long totalMessages = partitionDTOs.stream()
                    .mapToLong(TopicDetailDTO.PartitionDTO::getMessageCount)
                    .sum();
            
            return TopicDetailDTO.builder()
                    .name(description.name())
                    .partitionCount(description.partitions().size())
                    .replicationFactor(description.partitions().isEmpty() ? 0 : 
                            description.partitions().get(0).replicas().size())
                    .internal(description.isInternal())
                    .messageCount(totalMessages)
                    .partitions(partitionDTOs)
                    .build();
            
        } catch (ExecutionException | InterruptedException e) {
            log.error("Failed to get topic {} from cluster {}", topicName, clusterId, e);
            throw new BusinessException("Failed to get topic: " + e.getMessage());
        }
    }

    /**
     * Create topic
     */
    @CacheEvict(value = "topics", allEntries = true)
    public TopicDTO createTopic(String clusterId, TopicCreateRequest request) {
        permissionChecker.checkPermission(clusterId, Resource.TOPIC, Action.CREATE);
        
        try {
            kafkaAdminWrapper.createTopic(
                    clusterId,
                    request.getName(),
                    request.getPartitions(),
                    request.getReplicationFactor(),
                    request.getConfigs()
            );
            
            log.info("Created topic {} in cluster {}", request.getName(), clusterId);
            
            // Return created topic info
            return TopicDTO.builder()
                    .name(request.getName())
                    .partitionCount(request.getPartitions())
                    .replicationFactor(request.getReplicationFactor())
                    .internal(false)
                    .messageCount(0)
                    .build();
            
        } catch (ExecutionException | InterruptedException e) {
            log.error("Failed to create topic {} in cluster {}", request.getName(), clusterId, e);
            throw new BusinessException("Failed to create topic: " + e.getMessage());
        }
    }

    /**
     * Delete topic
     */
    @CacheEvict(value = {"topics", "topic"}, allEntries = true)
    public void deleteTopic(String clusterId, String topicName) {
        permissionChecker.checkPermission(clusterId, Resource.TOPIC, Action.DELETE, topicName);
        
        try {
            kafkaAdminWrapper.deleteTopic(clusterId, topicName);
            log.info("Deleted topic {} from cluster {}", topicName, clusterId);
        } catch (ExecutionException | InterruptedException e) {
            log.error("Failed to delete topic {} from cluster {}", topicName, clusterId, e);
            throw new BusinessException("Failed to delete topic: " + e.getMessage());
        }
    }

    /**
     * Increase partitions
     */
    @CacheEvict(value = "topic", key = "#clusterId + '-' + #topicName")
    public void increasePartitions(String clusterId, String topicName, int newPartitionCount) {
        permissionChecker.checkPermission(clusterId, Resource.TOPIC, Action.UPDATE, topicName);
        
        try {
            kafkaAdminWrapper.increasePartitions(clusterId, topicName, newPartitionCount);
            log.info("Increased partitions for topic {} to {} in cluster {}", topicName, newPartitionCount, clusterId);
        } catch (ExecutionException | InterruptedException e) {
            log.error("Failed to increase partitions for topic {} in cluster {}", topicName, clusterId, e);
            throw new BusinessException("Failed to increase partitions: " + e.getMessage());
        }
    }

    /**
     * Get topic configs
     */
    public List<TopicConfigDTO> getTopicConfigs(String clusterId, String topicName) {
        permissionChecker.checkPermission(clusterId, Resource.TOPIC, Action.READ, topicName);
        
        try {
            Collection<ConfigEntry> entries = kafkaAdminWrapper.getTopicConfigEntries(clusterId, topicName);
            
            return entries.stream()
                    .map(entry -> TopicConfigDTO.builder()
                            .name(entry.name())
                            .value(entry.value())
                            .isDefault(entry.isDefault())
                            .isReadOnly(entry.isReadOnly())
                            .isSensitive(entry.isSensitive())
                            .source(entry.source().name())
                            .documentation(entry.documentation())
                            .build())
                    .sorted(Comparator.comparing(TopicConfigDTO::getName))
                    .collect(Collectors.toList());
            
        } catch (ExecutionException | InterruptedException e) {
            log.error("Failed to get configs for topic {} in cluster {}", topicName, clusterId, e);
            throw new BusinessException("Failed to get topic configs: " + e.getMessage());
        }
    }

    /**
     * Update topic configs
     */
    @CacheEvict(value = "topic", key = "#clusterId + '-' + #topicName")
    public void updateTopicConfigs(String clusterId, String topicName, Map<String, String> configs) {
        permissionChecker.checkPermission(clusterId, Resource.TOPIC, Action.UPDATE, topicName);
        
        try {
            kafkaAdminWrapper.updateTopicConfigs(clusterId, topicName, configs);
            log.info("Updated configs for topic {} in cluster {}", topicName, clusterId);
        } catch (ExecutionException | InterruptedException e) {
            log.error("Failed to update configs for topic {} in cluster {}", topicName, clusterId, e);
            throw new BusinessException("Failed to update topic configs: " + e.getMessage());
        }
    }

    // ==================== Helper Methods ====================

    private boolean matchesSearch(String topicName, String search) {
        if (search == null || search.isBlank()) {
            return true;
        }
        return topicName.toLowerCase().contains(search.toLowerCase());
    }

    private boolean matchesView(String topicName, TopicListView view) {
        if (view == null || view == TopicListView.ALL) {
            return true;
        }
        
        boolean isInternal = isInternalTopic(topicName);
        boolean isStream = isStreamTopic(topicName);
        
        return switch (view) {
            case HIDE_INTERNAL -> !isInternal;
            case HIDE_STREAM -> !isStream;
            case HIDE_INTERNAL_STREAM -> !isInternal && !isStream;
            default -> true;
        };
    }

    private boolean isInternalTopic(String topicName) {
        return properties.getInternalTopicsRegex().stream()
                .anyMatch(regex -> Pattern.matches(regex, topicName));
    }

    private boolean isStreamTopic(String topicName) {
        return properties.getStreamTopicsRegex().stream()
                .anyMatch(regex -> Pattern.matches(regex, topicName));
    }

    private long calculateMessageCount(String topicName, List<TopicPartitionInfo> partitions,
                                        Map<TopicPartition, Long> beginOffsets,
                                        Map<TopicPartition, Long> endOffsets) {
        return partitions.stream()
                .mapToLong(p -> {
                    TopicPartition tp = new TopicPartition(topicName, p.partition());
                    long begin = beginOffsets.getOrDefault(tp, 0L);
                    long end = endOffsets.getOrDefault(tp, 0L);
                    return Math.max(0, end - begin);
                })
                .sum();
    }
}
