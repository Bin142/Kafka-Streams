package com.kafkamanagement.application.consumergroup;

import com.kafkamanagement.application.consumergroup.dto.*;
import com.kafkamanagement.common.exception.BusinessException;
import com.kafkamanagement.common.exception.ResourceNotFoundException;
import com.kafkamanagement.common.security.PermissionChecker;
import com.kafkamanagement.domain.user.model.Action;
import com.kafkamanagement.domain.user.model.Resource;
import com.kafkamanagement.infrastructure.kafka.KafkaAdminWrapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.ConsumerGroupDescription;
import org.apache.kafka.clients.admin.ConsumerGroupListing;
import org.apache.kafka.clients.admin.MemberDescription;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.ConsumerGroupState;
import org.apache.kafka.common.TopicPartition;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageImpl;
import org.springframework.data.domain.Pageable;
import org.springframework.stereotype.Service;

import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

@Slf4j
@Service
@RequiredArgsConstructor
public class ConsumerGroupService {

    private final KafkaAdminWrapper kafkaAdminWrapper;
    private final PermissionChecker permissionChecker;

    /**
     * List consumer groups with filtering and pagination
     */
    public Page<ConsumerGroupDTO> listConsumerGroups(String clusterId, Pageable pageable, 
                                                      String search, String stateFilter) {
        permissionChecker.checkPermission(clusterId, Resource.CONSUMER_GROUP, Action.READ);
        
        try {
            Collection<ConsumerGroupListing> allGroups = kafkaAdminWrapper.listConsumerGroups(clusterId);
            
            // Filter by search
            List<String> filteredGroupIds = allGroups.stream()
                    .map(ConsumerGroupListing::groupId)
                    .filter(id -> search == null || id.toLowerCase().contains(search.toLowerCase()))
                    .filter(id -> permissionChecker.hasPermission(clusterId, Resource.CONSUMER_GROUP, Action.READ, id))
                    .sorted(String::compareToIgnoreCase)
                    .collect(Collectors.toList());
            
            // Paginate
            int start = (int) pageable.getOffset();
            int end = Math.min(start + pageable.getPageSize(), filteredGroupIds.size());
            
            if (start > filteredGroupIds.size()) {
                return new PageImpl<>(Collections.emptyList(), pageable, filteredGroupIds.size());
            }
            
            List<String> pageGroupIds = filteredGroupIds.subList(start, end);
            
            // Get group details
            Map<String, ConsumerGroupDescription> descriptions = 
                    kafkaAdminWrapper.describeConsumerGroups(clusterId, pageGroupIds);
            
            // Filter by state if specified
            List<ConsumerGroupDTO> groups = descriptions.values().stream()
                    .filter(desc -> stateFilter == null || 
                            desc.state().name().equalsIgnoreCase(stateFilter))
                    .map(desc -> toConsumerGroupDTO(clusterId, desc))
                    .sorted(Comparator.comparing(ConsumerGroupDTO::getGroupId, String.CASE_INSENSITIVE_ORDER))
                    .collect(Collectors.toList());
            
            return new PageImpl<>(groups, pageable, filteredGroupIds.size());
            
        } catch (ExecutionException | InterruptedException e) {
            log.error("Failed to list consumer groups for cluster {}", clusterId, e);
            throw new BusinessException("Failed to list consumer groups: " + e.getMessage());
        }
    }

    /**
     * Get consumer group details
     */
    public ConsumerGroupDetailDTO getConsumerGroup(String clusterId, String groupId) {
        permissionChecker.checkPermission(clusterId, Resource.CONSUMER_GROUP, Action.READ, groupId);
        
        try {
            Map<String, ConsumerGroupDescription> descriptions = 
                    kafkaAdminWrapper.describeConsumerGroups(clusterId, List.of(groupId));
            
            ConsumerGroupDescription description = descriptions.get(groupId);
            if (description == null) {
                throw new ResourceNotFoundException("Consumer Group", groupId);
            }
            
            // Get offsets
            Map<TopicPartition, OffsetAndMetadata> offsets = 
                    kafkaAdminWrapper.getConsumerGroupOffsets(clusterId, groupId);
            
            // Get end offsets for lag calculation
            List<TopicPartition> partitions = new ArrayList<>(offsets.keySet());
            Map<TopicPartition, Long> endOffsets = kafkaAdminWrapper.getEndOffsets(clusterId, partitions);
            
            return toConsumerGroupDetailDTO(description, offsets, endOffsets);
            
        } catch (ExecutionException | InterruptedException e) {
            log.error("Failed to get consumer group {} from cluster {}", groupId, clusterId, e);
            throw new BusinessException("Failed to get consumer group: " + e.getMessage());
        }
    }

    /**
     * Delete consumer group
     */
    public void deleteConsumerGroup(String clusterId, String groupId) {
        permissionChecker.checkPermission(clusterId, Resource.CONSUMER_GROUP, Action.DELETE, groupId);
        
        try {
            // Check if group is empty
            Map<String, ConsumerGroupDescription> descriptions = 
                    kafkaAdminWrapper.describeConsumerGroups(clusterId, List.of(groupId));
            ConsumerGroupDescription description = descriptions.get(groupId);
            
            if (description != null && description.state() != ConsumerGroupState.EMPTY) {
                throw new BusinessException("Cannot delete consumer group with active members. " +
                        "Current state: " + description.state());
            }
            
            kafkaAdminWrapper.deleteConsumerGroup(clusterId, groupId);
            log.info("Deleted consumer group {} from cluster {}", groupId, clusterId);
            
        } catch (ExecutionException | InterruptedException e) {
            log.error("Failed to delete consumer group {} from cluster {}", groupId, clusterId, e);
            throw new BusinessException("Failed to delete consumer group: " + e.getMessage());
        }
    }

    /**
     * Reset consumer group offsets
     */
    public void resetOffsets(String clusterId, String groupId, OffsetResetRequest request) {
        permissionChecker.checkPermission(clusterId, Resource.CONSUMER_GROUP, Action.UPDATE, groupId);
        
        try {
            // Check if group is empty
            Map<String, ConsumerGroupDescription> descriptions = 
                    kafkaAdminWrapper.describeConsumerGroups(clusterId, List.of(groupId));
            ConsumerGroupDescription description = descriptions.get(groupId);
            
            if (description != null && description.state() != ConsumerGroupState.EMPTY) {
                throw new BusinessException("Cannot reset offsets for consumer group with active members. " +
                        "Please stop all consumers first. Current state: " + description.state());
            }
            
            // Get current offsets
            Map<TopicPartition, OffsetAndMetadata> currentOffsets = 
                    kafkaAdminWrapper.getConsumerGroupOffsets(clusterId, groupId);
            
            // Determine which partitions to reset
            Set<TopicPartition> partitionsToReset = determinePartitionsToReset(request, currentOffsets.keySet());
            
            // Calculate new offsets based on reset type
            Map<TopicPartition, OffsetAndMetadata> newOffsets = 
                    calculateNewOffsets(clusterId, request, partitionsToReset, currentOffsets);
            
            // Apply new offsets
            kafkaAdminWrapper.alterConsumerGroupOffsets(clusterId, groupId, newOffsets);
            
            log.info("Reset offsets for consumer group {} in cluster {} using strategy {}", 
                    groupId, clusterId, request.getType());
            
        } catch (ExecutionException | InterruptedException e) {
            log.error("Failed to reset offsets for consumer group {} in cluster {}", groupId, clusterId, e);
            throw new BusinessException("Failed to reset offsets: " + e.getMessage());
        }
    }

    // ==================== Helper Methods ====================

    private ConsumerGroupDTO toConsumerGroupDTO(String clusterId, ConsumerGroupDescription description) {
        long totalLag = 0;
        List<String> topics = new ArrayList<>();
        
        try {
            Map<TopicPartition, OffsetAndMetadata> offsets = 
                    kafkaAdminWrapper.getConsumerGroupOffsets(clusterId, description.groupId());
            
            topics = offsets.keySet().stream()
                    .map(TopicPartition::topic)
                    .distinct()
                    .sorted()
                    .collect(Collectors.toList());
            
            List<TopicPartition> partitions = new ArrayList<>(offsets.keySet());
            Map<TopicPartition, Long> endOffsets = kafkaAdminWrapper.getEndOffsets(clusterId, partitions);
            
            totalLag = offsets.entrySet().stream()
                    .mapToLong(e -> {
                        long endOffset = endOffsets.getOrDefault(e.getKey(), 0L);
                        long currentOffset = e.getValue().offset();
                        return Math.max(0, endOffset - currentOffset);
                    })
                    .sum();
        } catch (Exception e) {
            log.warn("Failed to get offsets for consumer group {}", description.groupId(), e);
        }
        
        return ConsumerGroupDTO.builder()
                .groupId(description.groupId())
                .state(description.state().name())
                .memberCount(description.members().size())
                .coordinator(description.coordinator() != null ? 
                        description.coordinator().host() + ":" + description.coordinator().port() : null)
                .partitionAssignor(description.partitionAssignor())
                .topics(topics)
                .totalLag(totalLag)
                .build();
    }

    private ConsumerGroupDetailDTO toConsumerGroupDetailDTO(ConsumerGroupDescription description,
                                                             Map<TopicPartition, OffsetAndMetadata> offsets,
                                                             Map<TopicPartition, Long> endOffsets) {
        List<ConsumerGroupDetailDTO.MemberDTO> members = description.members().stream()
                .map(member -> ConsumerGroupDetailDTO.MemberDTO.builder()
                        .memberId(member.consumerId())
                        .clientId(member.clientId())
                        .host(member.host())
                        .assignments(member.assignment().topicPartitions().stream()
                                .map(tp -> ConsumerGroupDetailDTO.AssignmentDTO.builder()
                                        .topic(tp.topic())
                                        .partition(tp.partition())
                                        .build())
                                .collect(Collectors.toList()))
                        .build())
                .collect(Collectors.toList());
        
        List<ConsumerGroupDetailDTO.OffsetDTO> offsetDTOs = offsets.entrySet().stream()
                .map(e -> {
                    TopicPartition tp = e.getKey();
                    OffsetAndMetadata om = e.getValue();
                    long endOffset = endOffsets.getOrDefault(tp, 0L);
                    long lag = Math.max(0, endOffset - om.offset());
                    
                    return ConsumerGroupDetailDTO.OffsetDTO.builder()
                            .topic(tp.topic())
                            .partition(tp.partition())
                            .currentOffset(om.offset())
                            .endOffset(endOffset)
                            .lag(lag)
                            .metadata(om.metadata())
                            .build();
                })
                .sorted(Comparator.comparing(ConsumerGroupDetailDTO.OffsetDTO::getTopic)
                        .thenComparingInt(ConsumerGroupDetailDTO.OffsetDTO::getPartition))
                .collect(Collectors.toList());
        
        long totalLag = offsetDTOs.stream().mapToLong(ConsumerGroupDetailDTO.OffsetDTO::getLag).sum();
        
        return ConsumerGroupDetailDTO.builder()
                .groupId(description.groupId())
                .state(description.state().name())
                .coordinator(description.coordinator() != null ? 
                        description.coordinator().host() + ":" + description.coordinator().port() : null)
                .partitionAssignor(description.partitionAssignor())
                .members(members)
                .offsets(offsetDTOs)
                .totalLag(totalLag)
                .build();
    }

    private Set<TopicPartition> determinePartitionsToReset(OffsetResetRequest request, 
                                                            Set<TopicPartition> allPartitions) {
        if (request.getPartitions() != null && !request.getPartitions().isEmpty()) {
            return request.getPartitions().stream()
                    .map(p -> new TopicPartition(p.getTopic(), p.getPartition()))
                    .collect(Collectors.toSet());
        }
        
        if (request.getTopics() != null && !request.getTopics().isEmpty()) {
            return allPartitions.stream()
                    .filter(tp -> request.getTopics().contains(tp.topic()))
                    .collect(Collectors.toSet());
        }
        
        return allPartitions;
    }

    private Map<TopicPartition, OffsetAndMetadata> calculateNewOffsets(String clusterId,
                                                                        OffsetResetRequest request,
                                                                        Set<TopicPartition> partitions,
                                                                        Map<TopicPartition, OffsetAndMetadata> currentOffsets) {
        List<TopicPartition> partitionList = new ArrayList<>(partitions);
        
        return switch (request.getType()) {
            case TO_BEGINNING -> {
                Map<TopicPartition, Long> beginOffsets = kafkaAdminWrapper.getBeginningOffsets(clusterId, partitionList);
                yield beginOffsets.entrySet().stream()
                        .collect(Collectors.toMap(Map.Entry::getKey, e -> new OffsetAndMetadata(e.getValue())));
            }
            case TO_END -> {
                Map<TopicPartition, Long> endOffsets = kafkaAdminWrapper.getEndOffsets(clusterId, partitionList);
                yield endOffsets.entrySet().stream()
                        .collect(Collectors.toMap(Map.Entry::getKey, e -> new OffsetAndMetadata(e.getValue())));
            }
            case TO_TIMESTAMP -> {
                if (request.getTimestamp() == null) {
                    throw new BusinessException("Timestamp is required for TO_TIMESTAMP reset type");
                }
                Map<TopicPartition, Long> timestampMap = partitions.stream()
                        .collect(Collectors.toMap(tp -> tp, tp -> request.getTimestamp()));
                Map<TopicPartition, Long> offsetsForTimes = kafkaAdminWrapper.getOffsetsForTimes(clusterId, timestampMap);
                yield offsetsForTimes.entrySet().stream()
                        .collect(Collectors.toMap(Map.Entry::getKey, e -> new OffsetAndMetadata(e.getValue())));
            }
            case TO_OFFSET -> {
                if (request.getOffsets() == null || request.getOffsets().isEmpty()) {
                    throw new BusinessException("Offsets map is required for TO_OFFSET reset type");
                }
                yield partitions.stream()
                        .filter(tp -> request.getOffsets().containsKey(tp.topic()) && 
                                request.getOffsets().get(tp.topic()).containsKey(tp.partition()))
                        .collect(Collectors.toMap(
                                tp -> tp,
                                tp -> new OffsetAndMetadata(request.getOffsets().get(tp.topic()).get(tp.partition()))
                        ));
            }
            case SHIFT_BY -> {
                if (request.getShiftBy() == null) {
                    throw new BusinessException("ShiftBy value is required for SHIFT_BY reset type");
                }
                yield partitions.stream()
                        .filter(currentOffsets::containsKey)
                        .collect(Collectors.toMap(
                                tp -> tp,
                                tp -> new OffsetAndMetadata(
                                        Math.max(0, currentOffsets.get(tp).offset() + request.getShiftBy())
                                )
                        ));
            }
        };
    }
}
