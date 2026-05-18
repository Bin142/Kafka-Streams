package com.kafkamanagement.application.user.dto;

import jakarta.validation.constraints.NotBlank;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Set;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class PermissionCreateRequest {
    
    @NotBlank(message = "Resource is required")
    private String resource;  // CLUSTER, TOPIC, TOPIC_DATA, CONSUMER_GROUP, SCHEMA, CONNECT, ACL, USER, ROLE
    
    @NotBlank(message = "Action is required")
    private String action;  // READ, CREATE, UPDATE, DELETE, MANAGE
    
    private String resourcePattern;  // Glob pattern (e.g., dev-*, prod-orders-*)
    
    private Set<String> clusterIds;  // Specific cluster IDs
    
    private String description;
}
