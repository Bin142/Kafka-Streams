package com.kafkamanagement.application.schema.dto;

import jakarta.validation.constraints.NotBlank;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class SchemaCreateRequest {
    
    @NotBlank(message = "Subject is required")
    private String subject;
    
    @NotBlank(message = "Schema is required")
    private String schema;
    
    @Builder.Default
    private String schemaType = "AVRO";  // AVRO, JSON, PROTOBUF
    
    private String compatibility;  // BACKWARD, FORWARD, FULL, NONE, etc.
}
