package com.kafkamanagement.application.connector.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;
import java.util.Map;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class ConnectorDTO {
    private String name;
    private String type; // SOURCE or SINK
    private String state; // RUNNING, PAUSED, FAILED, UNASSIGNED
    private String workerUrl;
    private Map<String, String> config;
    private List<TaskDTO> tasks;

    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    public static class TaskDTO {
        private int id;
        private String state;
        private String workerId;
        private String trace; // Error trace if failed
    }
}
