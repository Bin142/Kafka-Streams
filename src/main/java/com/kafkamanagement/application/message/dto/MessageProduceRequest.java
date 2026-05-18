package com.kafkamanagement.application.message.dto;

import jakarta.validation.constraints.NotNull;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Map;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class MessageProduceRequest {
    
    private String key;
    
    @NotNull(message = "Value is required")
    private String value;
    
    private Map<String, String> headers;
    
    private Integer partition;  // null = let Kafka decide
    
    private Long timestamp;  // null = current time
    
    // Schema info for Avro/Protobuf/JSON Schema
    private Integer keySchemaId;
    private Integer valueSchemaId;
    
    public enum ValueFormat {
        STRING,
        JSON,
        AVRO,
        PROTOBUF
    }
    
    @Builder.Default
    private ValueFormat keyFormat = ValueFormat.STRING;
    
    @Builder.Default
    private ValueFormat valueFormat = ValueFormat.STRING;
}
