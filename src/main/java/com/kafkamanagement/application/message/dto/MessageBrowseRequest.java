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
public class MessageBrowseRequest {
    
    public enum SortOrder {
        OLDEST,  // From beginning
        NEWEST   // From end
    }
    
    @Builder.Default
    private SortOrder sort = SortOrder.NEWEST;
    
    private Integer partition;  // null = all partitions
    
    private Instant afterTimestamp;
    private Instant beforeTimestamp;
    
    private String keyContains;
    private String valueContains;
    
    private String headerKey;
    private String headerValue;
    
    @Builder.Default
    private int limit = 100;
    
    // Cursor for pagination (partition:offset format)
    private String cursor;
}
