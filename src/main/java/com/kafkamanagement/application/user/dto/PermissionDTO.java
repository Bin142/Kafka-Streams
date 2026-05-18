package com.kafkamanagement.application.user.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.Instant;
import java.util.Set;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class PermissionDTO {
    private Long id;
    private String resource;
    private String action;
    private String resourcePattern;
    private Set<String> clusterIds;
    private String description;
    private Instant createdAt;
}
