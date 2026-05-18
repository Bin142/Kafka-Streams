package com.kafkamanagement.application.consumergroup.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class ConsumerGroupDTO {
    private String groupId;
    private String state;
    private int memberCount;
    private String coordinator;
    private String partitionAssignor;
    private List<String> topics;
    private long totalLag;
}
