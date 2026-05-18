package com.kafkamanagement.application.message.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.Instant;
import java.util.Map;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class MessageDTO {
    private int partition;
    private long offset;
    private Instant timestamp;
    private String timestampType;
    private String key;
    private String value;
    private Map<String, String> headers;
    private String keyFormat;
    private String valueFormat;
    private Integer keySchemaId;
    private Integer valueSchemaId;
}
