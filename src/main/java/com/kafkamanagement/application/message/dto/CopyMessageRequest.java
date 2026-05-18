package com.kafkamanagement.application.message.dto;

import jakarta.validation.constraints.NotBlank;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.Instant;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class CopyMessageRequest {
    
    @NotBlank(message = "Destination cluster ID is required")
    private String destinationClusterId;
    
    @NotBlank(message = "Destination topic is required")
    private String destinationTopic;
    
    // Filters
    private String keyContains;
    private Instant afterTimestamp;
    private Instant beforeTimestamp;
    
    @Builder.Default
    private int limit = 1000;  // Max messages to copy
    
    @Builder.Default
    private boolean preserveTimestamp = true;
    
    @Builder.Default
    private boolean preservePartition = false;
}
