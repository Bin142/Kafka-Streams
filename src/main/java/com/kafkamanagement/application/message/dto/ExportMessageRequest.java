package com.kafkamanagement.application.message.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.Instant;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class ExportMessageRequest {
    
    public enum ExportFormat {
        JSON,
        CSV
    }
    
    @Builder.Default
    private ExportFormat format = ExportFormat.JSON;
    
    private Integer partition;
    
    private Instant afterTimestamp;
    private Instant beforeTimestamp;
    
    private String keyContains;
    private String valueContains;
    
    @Builder.Default
    private int limit = 10000;  // Max records to export
    
    @Builder.Default
    private boolean includeHeaders = true;
    
    @Builder.Default
    private boolean includeMetadata = true;  // partition, offset, timestamp
}
