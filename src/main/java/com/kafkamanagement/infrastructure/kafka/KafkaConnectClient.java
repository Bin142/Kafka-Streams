package com.kafkamanagement.infrastructure.kafka;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.kafkamanagement.application.connector.dto.ConnectorDTO;
import com.kafkamanagement.application.connector.dto.ConnectorPluginDTO;
import com.kafkamanagement.infrastructure.kafka.config.ClusterConfig;
import com.kafkamanagement.infrastructure.kafka.config.ClustersConfigLoader;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.*;
import org.springframework.stereotype.Component;
import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.client.RestTemplate;

import java.util.*;
import java.util.stream.Collectors;

@Slf4j
@Component
@RequiredArgsConstructor
public class KafkaConnectClient {

    private final ClustersConfigLoader clustersConfigLoader;
    private final RestTemplate restTemplate = new RestTemplate();
    private final ObjectMapper objectMapper = new ObjectMapper();

    private String getConnectUrl(String clusterId) {
        ClusterConfig config = clustersConfigLoader.getCluster(clusterId)
                .orElseThrow(() -> new IllegalStateException("Cluster not found: " + clusterId));
        String connectUrl = config.getDefaultConnectUrl();
        if (connectUrl == null) {
            throw new IllegalStateException("Kafka Connect URL not configured for cluster: " + clusterId);
        }
        return connectUrl;
    }

    /**
     * List all connectors
     */
    @SuppressWarnings("unchecked")
    public List<ConnectorDTO> listConnectors(String clusterId) {
        String url = getConnectUrl(clusterId);
        try {
            // Get connector names
            ResponseEntity<List> response = restTemplate.getForEntity(url + "/connectors", List.class);
            List<String> connectorNames = response.getBody();
            
            if (connectorNames == null || connectorNames.isEmpty()) {
                return Collections.emptyList();
            }

            // Get details for each connector
            return connectorNames.stream()
                    .map(name -> getConnector(clusterId, (String) name).orElse(null))
                    .filter(Objects::nonNull)
                    .collect(Collectors.toList());
        } catch (Exception e) {
            log.error("Failed to list connectors for cluster: {}", clusterId, e);
            return Collections.emptyList();
        }
    }

    /**
     * Get connector details
     */
    @SuppressWarnings("unchecked")
    public Optional<ConnectorDTO> getConnector(String clusterId, String connectorName) {
        String url = getConnectUrl(clusterId);
        try {
            // Get connector info
            ResponseEntity<Map> infoResponse = restTemplate.getForEntity(
                    url + "/connectors/" + connectorName, Map.class);
            Map<String, Object> info = infoResponse.getBody();

            // Get connector status
            ResponseEntity<Map> statusResponse = restTemplate.getForEntity(
                    url + "/connectors/" + connectorName + "/status", Map.class);
            Map<String, Object> status = statusResponse.getBody();

            return Optional.of(buildConnectorDTO(info, status));
        } catch (HttpClientErrorException.NotFound e) {
            return Optional.empty();
        } catch (Exception e) {
            log.error("Failed to get connector: {} from cluster: {}", connectorName, clusterId, e);
            return Optional.empty();
        }
    }

    /**
     * Create connector
     */
    @SuppressWarnings("unchecked")
    public ConnectorDTO createConnector(String clusterId, String name, Map<String, String> config) {
        String url = getConnectUrl(clusterId);
        
        Map<String, Object> request = new HashMap<>();
        request.put("name", name);
        request.put("config", config);

        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        HttpEntity<Map<String, Object>> entity = new HttpEntity<>(request, headers);

        ResponseEntity<Map> response = restTemplate.postForEntity(url + "/connectors", entity, Map.class);
        
        // Return the created connector
        return getConnector(clusterId, name).orElseThrow();
    }

    /**
     * Update connector config
     */
    @SuppressWarnings("unchecked")
    public ConnectorDTO updateConnectorConfig(String clusterId, String connectorName, Map<String, String> config) {
        String url = getConnectUrl(clusterId);

        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        HttpEntity<Map<String, String>> entity = new HttpEntity<>(config, headers);

        restTemplate.put(url + "/connectors/" + connectorName + "/config", entity);
        
        return getConnector(clusterId, connectorName).orElseThrow();
    }

    /**
     * Delete connector
     */
    public void deleteConnector(String clusterId, String connectorName) {
        String url = getConnectUrl(clusterId);
        restTemplate.delete(url + "/connectors/" + connectorName);
    }

    /**
     * Restart connector
     */
    public void restartConnector(String clusterId, String connectorName) {
        String url = getConnectUrl(clusterId);
        restTemplate.postForEntity(url + "/connectors/" + connectorName + "/restart", null, Void.class);
    }

    /**
     * Pause connector
     */
    public void pauseConnector(String clusterId, String connectorName) {
        String url = getConnectUrl(clusterId);
        restTemplate.put(url + "/connectors/" + connectorName + "/pause", null);
    }

    /**
     * Resume connector
     */
    public void resumeConnector(String clusterId, String connectorName) {
        String url = getConnectUrl(clusterId);
        restTemplate.put(url + "/connectors/" + connectorName + "/resume", null);
    }

    /**
     * Restart specific task
     */
    public void restartTask(String clusterId, String connectorName, int taskId) {
        String url = getConnectUrl(clusterId);
        restTemplate.postForEntity(
                url + "/connectors/" + connectorName + "/tasks/" + taskId + "/restart",
                null, Void.class);
    }

    /**
     * List available plugins
     */
    @SuppressWarnings("unchecked")
    public List<ConnectorPluginDTO> listPlugins(String clusterId) {
        String url = getConnectUrl(clusterId);
        try {
            ResponseEntity<List> response = restTemplate.getForEntity(url + "/connector-plugins", List.class);
            List<Map<String, Object>> plugins = response.getBody();
            
            if (plugins == null) {
                return Collections.emptyList();
            }

            return plugins.stream()
                    .map(this::buildPluginDTO)
                    .collect(Collectors.toList());
        } catch (Exception e) {
            log.error("Failed to list plugins for cluster: {}", clusterId, e);
            return Collections.emptyList();
        }
    }

    /**
     * Validate connector config
     */
    @SuppressWarnings("unchecked")
    public Map<String, Object> validateConfig(String clusterId, String pluginName, Map<String, String> config) {
        String url = getConnectUrl(clusterId);

        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        HttpEntity<Map<String, String>> entity = new HttpEntity<>(config, headers);

        ResponseEntity<Map> response = restTemplate.exchange(
                url + "/connector-plugins/" + pluginName + "/config/validate",
                HttpMethod.PUT, entity, Map.class);
        
        return response.getBody();
    }

    @SuppressWarnings("unchecked")
    private ConnectorDTO buildConnectorDTO(Map<String, Object> info, Map<String, Object> status) {
        Map<String, Object> connectorStatus = (Map<String, Object>) status.get("connector");
        List<Map<String, Object>> taskStatuses = (List<Map<String, Object>>) status.get("tasks");

        List<ConnectorDTO.TaskDTO> tasks = taskStatuses != null ? taskStatuses.stream()
                .map(task -> ConnectorDTO.TaskDTO.builder()
                        .id((Integer) task.get("id"))
                        .state((String) task.get("state"))
                        .workerId((String) task.get("worker_id"))
                        .trace((String) task.get("trace"))
                        .build())
                .collect(Collectors.toList()) : Collections.emptyList();

        Map<String, String> config = (Map<String, String>) info.get("config");
        String type = config != null ? config.getOrDefault("connector.class", "unknown") : "unknown";
        
        // Determine if source or sink
        String connectorType = "UNKNOWN";
        if (type.toLowerCase().contains("source")) {
            connectorType = "SOURCE";
        } else if (type.toLowerCase().contains("sink")) {
            connectorType = "SINK";
        }

        return ConnectorDTO.builder()
                .name((String) info.get("name"))
                .type(connectorType)
                .state(connectorStatus != null ? (String) connectorStatus.get("state") : "UNKNOWN")
                .workerUrl(connectorStatus != null ? (String) connectorStatus.get("worker_id") : null)
                .config(config)
                .tasks(tasks)
                .build();
    }

    private ConnectorPluginDTO buildPluginDTO(Map<String, Object> plugin) {
        return ConnectorPluginDTO.builder()
                .className((String) plugin.get("class"))
                .type((String) plugin.get("type"))
                .version((String) plugin.get("version"))
                .build();
    }
}
