package com.kafkamanagement.application.cluster;

import com.kafkamanagement.application.cluster.dto.ClusterDTO;
import com.kafkamanagement.application.cluster.dto.ClusterDetailDTO;
import com.kafkamanagement.common.exception.ResourceNotFoundException;
import com.kafkamanagement.common.security.PermissionChecker;
import com.kafkamanagement.domain.user.model.Action;
import com.kafkamanagement.domain.user.model.Resource;
import com.kafkamanagement.infrastructure.kafka.KafkaAdminWrapper;
import com.kafkamanagement.infrastructure.kafka.KafkaClientFactory;
import com.kafkamanagement.infrastructure.kafka.config.ClusterConfig;
import com.kafkamanagement.infrastructure.kafka.config.ClustersConfigLoader;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.Node;
import org.springframework.cache.annotation.Cacheable;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

@Slf4j
@Service
@RequiredArgsConstructor
public class ClusterService {

    private final ClustersConfigLoader clustersConfigLoader;
    private final KafkaClientFactory kafkaClientFactory;
    private final KafkaAdminWrapper kafkaAdminWrapper;
    private final PermissionChecker permissionChecker;

    /**
     * List all clusters that user has access to
     */
    public List<ClusterDTO> listClusters() {
        List<ClusterConfig> allClusters = clustersConfigLoader.getAllClusters();
        
        return allClusters.stream()
                .filter(cluster -> permissionChecker.hasPermission(
                        cluster.getId(), Resource.CLUSTER, Action.READ))
                .map(this::toClusterDTO)
                .collect(Collectors.toList());
    }

    /**
     * Get cluster details
     */
    @Cacheable(value = "cluster", key = "#clusterId")
    public ClusterDetailDTO getCluster(String clusterId) {
        permissionChecker.checkPermission(clusterId, Resource.CLUSTER, Action.READ);
        
        ClusterConfig config = clustersConfigLoader.getCluster(clusterId)
                .orElseThrow(() -> new ResourceNotFoundException("Cluster", clusterId));
        
        return toClusterDetailDTO(config);
    }

    /**
     * Get cluster nodes
     */
    public List<ClusterDetailDTO.NodeDTO> getClusterNodes(String clusterId) {
        permissionChecker.checkPermission(clusterId, Resource.CLUSTER, Action.READ);
        
        try {
            Collection<Node> nodes = kafkaAdminWrapper.getNodes(clusterId);
            Node controller = kafkaAdminWrapper.getController(clusterId);
            
            return nodes.stream()
                    .map(node -> ClusterDetailDTO.NodeDTO.builder()
                            .id(node.id())
                            .host(node.host())
                            .port(node.port())
                            .rack(node.rack())
                            .isController(controller != null && node.id() == controller.id())
                            .build())
                    .collect(Collectors.toList());
        } catch (Exception e) {
            log.error("Failed to get nodes for cluster {}", clusterId, e);
            return new ArrayList<>();
        }
    }

    /**
     * Get node details by node ID
     */
    public ClusterDetailDTO.NodeDTO getNodeDetail(String clusterId, int nodeId) {
        permissionChecker.checkPermission(clusterId, Resource.CLUSTER, Action.READ);
        
        try {
            Collection<Node> nodes = kafkaAdminWrapper.getNodes(clusterId);
            Node controller = kafkaAdminWrapper.getController(clusterId);
            
            return nodes.stream()
                    .filter(node -> node.id() == nodeId)
                    .findFirst()
                    .map(node -> ClusterDetailDTO.NodeDTO.builder()
                            .id(node.id())
                            .host(node.host())
                            .port(node.port())
                            .rack(node.rack())
                            .isController(controller != null && node.id() == controller.id())
                            .build())
                    .orElseThrow(() -> new ResourceNotFoundException("Node", String.valueOf(nodeId)));
        } catch (ResourceNotFoundException e) {
            throw e;
        } catch (Exception e) {
            log.error("Failed to get node {} for cluster {}", nodeId, clusterId, e);
            throw new RuntimeException("Failed to get node details: " + e.getMessage());
        }
    }

    /**
     * Get node (broker) configs
     */
    public List<ClusterDetailDTO.ConfigEntryDTO> getNodeConfigs(String clusterId, int nodeId) {
        permissionChecker.checkPermission(clusterId, Resource.CLUSTER, Action.READ);
        
        try {
            Collection<org.apache.kafka.clients.admin.ConfigEntry> configs = 
                    kafkaAdminWrapper.getBrokerConfigs(clusterId, nodeId);
            
            return configs.stream()
                    .map(entry -> ClusterDetailDTO.ConfigEntryDTO.builder()
                            .name(entry.name())
                            .value(entry.value())
                            .source(entry.source().name())
                            .isDefault(entry.isDefault())
                            .isSensitive(entry.isSensitive())
                            .isReadOnly(entry.isReadOnly())
                            .build())
                    .sorted((a, b) -> a.getName().compareToIgnoreCase(b.getName()))
                    .collect(Collectors.toList());
        } catch (Exception e) {
            log.error("Failed to get configs for node {} in cluster {}", nodeId, clusterId, e);
            throw new RuntimeException("Failed to get node configs: " + e.getMessage());
        }
    }

    private ClusterDTO toClusterDTO(ClusterConfig config) {
        String status = "UNKNOWN";
        int nodeCount = 0;
        Integer controllerId = null;
        String kafkaClusterId = null;
        
        try {
            Collection<Node> nodes = kafkaAdminWrapper.getNodes(config.getId());
            Node controller = kafkaAdminWrapper.getController(config.getId());
            kafkaClusterId = kafkaAdminWrapper.getClusterId(config.getId());
            
            nodeCount = nodes.size();
            controllerId = controller != null ? controller.id() : null;
            status = "CONNECTED";
        } catch (Exception e) {
            log.warn("Failed to connect to cluster {}: {}", config.getId(), e.getMessage());
            status = "DISCONNECTED";
        }
        
        return ClusterDTO.builder()
                .id(config.getId())
                .name(config.getName())
                .bootstrapServers(config.getBootstrapServers())
                .kafkaClusterId(kafkaClusterId)
                .nodeCount(nodeCount)
                .controllerId(controllerId)
                .hasSchemaRegistry(config.getSchemaRegistry() != null)
                .hasConnect(config.getConnect() != null && !config.getConnect().isEmpty())
                .status(status)
                .build();
    }

    private ClusterDetailDTO toClusterDetailDTO(ClusterConfig config) {
        List<ClusterDetailDTO.NodeDTO> nodes = new ArrayList<>();
        Integer controllerId = null;
        String kafkaClusterId = null;
        
        try {
            Collection<Node> kafkaNodes = kafkaAdminWrapper.getNodes(config.getId());
            Node controller = kafkaAdminWrapper.getController(config.getId());
            kafkaClusterId = kafkaAdminWrapper.getClusterId(config.getId());
            controllerId = controller != null ? controller.id() : null;
            
            final Integer finalControllerId = controllerId;
            nodes = kafkaNodes.stream()
                    .map(node -> ClusterDetailDTO.NodeDTO.builder()
                            .id(node.id())
                            .host(node.host())
                            .port(node.port())
                            .rack(node.rack())
                            .isController(finalControllerId != null && node.id() == finalControllerId)
                            .build())
                    .collect(Collectors.toList());
        } catch (Exception e) {
            log.warn("Failed to get cluster details for {}: {}", config.getId(), e.getMessage());
        }
        
        ClusterDetailDTO.SchemaRegistryInfo schemaRegistryInfo = null;
        if (config.getSchemaRegistry() != null) {
            schemaRegistryInfo = ClusterDetailDTO.SchemaRegistryInfo.builder()
                    .url(config.getSchemaRegistry().getUrl())
                    .connected(true) // TODO: Check actual connection
                    .build();
        }
        
        List<ClusterDetailDTO.ConnectInfo> connects = new ArrayList<>();
        if (config.getConnect() != null) {
            connects = config.getConnect().stream()
                    .map(c -> ClusterDetailDTO.ConnectInfo.builder()
                            .name(c.getName())
                            .url(c.getUrl())
                            .connected(true) // TODO: Check actual connection
                            .build())
                    .collect(Collectors.toList());
        }
        
        return ClusterDetailDTO.builder()
                .id(config.getId())
                .name(config.getName())
                .bootstrapServers(config.getBootstrapServers())
                .kafkaClusterId(kafkaClusterId)
                .nodes(nodes)
                .controllerId(controllerId)
                .schemaRegistry(schemaRegistryInfo)
                .connects(connects)
                .build();
    }
}
