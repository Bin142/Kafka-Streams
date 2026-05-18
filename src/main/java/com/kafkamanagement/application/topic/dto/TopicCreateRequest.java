package com.kafkamanagement.application.topic.dto;

import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Pattern;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Map;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class TopicCreateRequest {

    @NotBlank(message = "Topic name is required")
    @Pattern(regexp = "^[a-zA-Z0-9._-]+$", message = "Topic name can only contain letters, numbers, dots, underscores, and hyphens")
    private String name;

    @Min(value = 1, message = "Partitions must be at least 1")
    @Builder.Default
    private int partitions = 1;

    @Min(value = 1, message = "Replication factor must be at least 1")
    @Builder.Default
    private short replicationFactor = 1;

    private Map<String, String> configs;
}
