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
public class ClusterDetailDTO {
    private String id;
    private String name;
    private String bootstrapServers;
    private String kafkaClusterId;
    private List<NodeDTO> nodes;
    private Integer controllerId;
    private SchemaRegistryInfo schemaRegistry;
    private List<ConnectInfo> connects;

    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    public static class NodeDTO {
        private int id;
        private String host;
        private int port;
        private String rack;
        private boolean isController;
    }

    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    public static class SchemaRegistryInfo {
        private String url;
        private boolean connected;
    }

    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    public static class ConnectInfo {
        private String name;
        private String url;
        private boolean connected;
    }
}
