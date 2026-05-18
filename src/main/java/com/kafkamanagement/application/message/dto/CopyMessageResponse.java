package com.kafkamanagement.application.message.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class CopyMessageResponse {
    private long copiedCount;
    private long failedCount;
    private String sourceClusterId;
    private String sourceTopic;
    private String destinationClusterId;
    private String destinationTopic;
    private long durationMs;
}
