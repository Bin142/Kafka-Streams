package com.kafkamanagement.application.acl.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class AclDTO {
    private String resourceType;      // TOPIC, GROUP, CLUSTER, TRANSACTIONAL_ID, DELEGATION_TOKEN
    private String resourceName;
    private String patternType;       // LITERAL, PREFIXED, MATCH, ANY
    private String principal;         // User:xxx or Group:xxx
    private String host;
    private String operation;         // READ, WRITE, CREATE, DELETE, ALTER, DESCRIBE, etc.
    private String permissionType;    // ALLOW, DENY
}
