package com.kafkamanagement.application.topic.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class TopicDetailDTO {
    private String name;
    private int partitionCount;
    private int replicationFactor;
    private boolean internal;
    private long messageCount;
    private List<PartitionDTO> partitions;

    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    public static class PartitionDTO {
        private int partition;
        private int leader;
        private List<Integer> replicas;
        private List<Integer> isr;
        private long beginningOffset;
        private long endOffset;
        private long messageCount;
    }
}
