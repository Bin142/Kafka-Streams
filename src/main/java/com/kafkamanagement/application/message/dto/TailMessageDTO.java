package com.kafkamanagement.application.message.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Map;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class TailMessageDTO {
    private int partition;
    private long offset;
    private long timestamp;
    private String timestampType;
    private String key;
    private String value;
    private Map<String, String> headers;
    private String keyFormat;
    private String valueFormat;
}
