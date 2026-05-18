package com.kafkamanagement.application.acl.dto;

import jakarta.validation.constraints.NotBlank;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class AclCreateRequest {
    
    @NotBlank(message = "Resource type is required")
    private String resourceType;      // TOPIC, GROUP, CLUSTER, TRANSACTIONAL_ID, DELEGATION_TOKEN
    
    @NotBlank(message = "Resource name is required")
    private String resourceName;
    
    private String patternType = "LITERAL";  // LITERAL, PREFIXED
    
    @NotBlank(message = "Principal is required")
    private String principal;         // User:xxx
    
    private String host = "*";
    
    @NotBlank(message = "Operation is required")
    private String operation;         // READ, WRITE, CREATE, DELETE, ALTER, DESCRIBE, etc.
    
    private String permissionType = "ALLOW";  // ALLOW, DENY
}
