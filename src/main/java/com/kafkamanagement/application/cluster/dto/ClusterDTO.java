package com.kafkamanagement.application.cluster.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class ClusterDTO {
    private String id;
    private String name;
    private String bootstrapServers;
    private String kafkaClusterId;
    private int nodeCount;
    private Integer controllerId;
    private boolean hasSchemaRegistry;
    private boolean hasConnect;
    private String status;
}
