package com.kafkamanagement.application.acl.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class AclDeleteRequest {
    private String resourceType;      // TOPIC, GROUP, CLUSTER, etc. (null = any)
    private String resourceName;      // null = any
    private String patternType;       // LITERAL, PREFIXED, ANY (null = any)
    private String principal;         // null = any
    private String host;              // null = any
    private String operation;         // null = any
    private String permissionType;    // ALLOW, DENY (null = any)
}
