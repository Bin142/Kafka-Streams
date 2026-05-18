package com.kafkamanagement.application.topic.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class TopicDTO {
    private String name;
    private int partitionCount;
    private int replicationFactor;
    private boolean internal;
    private long messageCount;
}
