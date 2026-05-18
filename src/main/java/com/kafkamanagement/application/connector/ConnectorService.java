package com.kafkamanagement.application.connector;

import com.kafkamanagement.application.connector.dto.ConnectorCreateRequest;
import com.kafkamanagement.application.connector.dto.ConnectorDTO;
import com.kafkamanagement.application.connector.dto.ConnectorPluginDTO;
import com.kafkamanagement.common.exception.ResourceNotFoundException;
import com.kafkamanagement.infrastructure.kafka.KafkaConnectClient;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map;

@Slf4j
@Service
@RequiredArgsConstructor
public class ConnectorService {

    private final KafkaConnectClient kafkaConnectClient;

    /**
     * List all connectors
     */
    public List<ConnectorDTO> listConnectors(String clusterId) {
        log.debug("Listing connectors for cluster: {}", clusterId);
        return kafkaConnectClient.listConnectors(clusterId);
    }

    /**
     * Get connector details
     */
    public ConnectorDTO getConnector(String clusterId, String connectorName) {
        log.debug("Getting connector: {} from cluster: {}", connectorName, clusterId);
        return kafkaConnectClient.getConnector(clusterId, connectorName)
                .orElseThrow(() -> new ResourceNotFoundException("Connector", connectorName));
    }

    /**
     * Create new connector
     */
    public ConnectorDTO createConnector(String clusterId, ConnectorCreateRequest request) {
        log.info("Creating connector: {} in cluster: {}", request.getName(), clusterId);
        return kafkaConnectClient.createConnector(clusterId, request.getName(), request.getConfig());
    }

    /**
     * Update connector config
     */
    public ConnectorDTO updateConnectorConfig(String clusterId, String connectorName, Map<String, String> config) {
        log.info("Updating connector config: {} in cluster: {}", connectorName, clusterId);
        return kafkaConnectClient.updateConnectorConfig(clusterId, connectorName, config);
    }

    /**
     * Delete connector
     */
    public void deleteConnector(String clusterId, String connectorName) {
        log.info("Deleting connector: {} from cluster: {}", connectorName, clusterId);
        kafkaConnectClient.deleteConnector(clusterId, connectorName);
    }

    /**
     * Restart connector
     */
    public void restartConnector(String clusterId, String connectorName) {
        log.info("Restarting connector: {} in cluster: {}", connectorName, clusterId);
        kafkaConnectClient.restartConnector(clusterId, connectorName);
    }

    /**
     * Pause connector
     */
    public void pauseConnector(String clusterId, String connectorName) {
        log.info("Pausing connector: {} in cluster: {}", connectorName, clusterId);
        kafkaConnectClient.pauseConnector(clusterId, connectorName);
    }

    /**
     * Resume connector
     */
    public void resumeConnector(String clusterId, String connectorName) {
        log.info("Resuming connector: {} in cluster: {}", connectorName, clusterId);
        kafkaConnectClient.resumeConnector(clusterId, connectorName);
    }

    /**
     * Restart specific task
     */
    public void restartTask(String clusterId, String connectorName, int taskId) {
        log.info("Restarting task {} of connector: {} in cluster: {}", taskId, connectorName, clusterId);
        kafkaConnectClient.restartTask(clusterId, connectorName, taskId);
    }

    /**
     * List available plugins
     */
    public List<ConnectorPluginDTO> listPlugins(String clusterId) {
        log.debug("Listing plugins for cluster: {}", clusterId);
        return kafkaConnectClient.listPlugins(clusterId);
    }

    /**
     * Validate connector config
     */
    public Map<String, Object> validateConfig(String clusterId, String pluginName, Map<String, String> config) {
        log.debug("Validating config for plugin: {} in cluster: {}", pluginName, clusterId);
        return kafkaConnectClient.validateConfig(clusterId, pluginName, config);
    }
}
