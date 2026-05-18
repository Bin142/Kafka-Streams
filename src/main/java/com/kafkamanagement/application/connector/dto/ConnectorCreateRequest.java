package com.kafkamanagement.application.connector.dto;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotEmpty;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Map;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class ConnectorCreateRequest {
    @NotBlank(message = "Connector name is required")
    private String name;

    @NotEmpty(message = "Connector config is required")
    private Map<String, String> config;
}
