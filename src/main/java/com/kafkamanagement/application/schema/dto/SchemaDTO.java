package com.kafkamanagement.application.schema.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class SchemaDTO {
    private String subject;
    private int version;
    private int id;
    private String schemaType;  // AVRO, JSON, PROTOBUF
    private String schema;
    private String compatibility;
}
