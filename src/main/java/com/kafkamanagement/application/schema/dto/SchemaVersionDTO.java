package com.kafkamanagement.application.schema.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class SchemaVersionDTO {
    private String subject;
    private int version;
    private int id;
    private String schemaType;
    private String schema;
}
