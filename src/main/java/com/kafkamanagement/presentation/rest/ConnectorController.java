package com.kafkamanagement.presentation.rest;

import com.kafkamanagement.application.connector.ConnectorService;
import com.kafkamanagement.application.connector.dto.ConnectorCreateRequest;
import com.kafkamanagement.application.connector.dto.ConnectorDTO;
import com.kafkamanagement.application.connector.dto.ConnectorPluginDTO;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/v1/clusters/{clusterId}/connectors")
@RequiredArgsConstructor
@Tag(name = "Kafka Connect", description = "Kafka Connect management APIs")
public class ConnectorController {

    private final ConnectorService connectorService;

    @GetMapping
    @Operation(summary = "List connectors", description = "List all connectors in the Kafka Connect cluster")
    public ResponseEntity<List<ConnectorDTO>> listConnectors(@PathVariable String clusterId) {
        return ResponseEntity.ok(connectorService.listConnectors(clusterId));
    }

    @PostMapping
    @Operation(summary = "Create connector", description = "Create a new connector")
    public ResponseEntity<ConnectorDTO> createConnector(
            @PathVariable String clusterId,
            @Valid @RequestBody ConnectorCreateRequest request) {
        return ResponseEntity.status(HttpStatus.CREATED)
                .body(connectorService.createConnector(clusterId, request));
    }

    @GetMapping("/{connectorName}")
    @Operation(summary = "Get connector", description = "Get connector details")
    public ResponseEntity<ConnectorDTO> getConnector(
            @PathVariable String clusterId,
            @PathVariable String connectorName) {
        return ResponseEntity.ok(connectorService.getConnector(clusterId, connectorName));
    }

    @PutMapping("/{connectorName}/config")
    @Operation(summary = "Update connector config", description = "Update connector configuration")
    public ResponseEntity<ConnectorDTO> updateConnectorConfig(
            @PathVariable String clusterId,
            @PathVariable String connectorName,
            @RequestBody Map<String, String> config) {
        return ResponseEntity.ok(connectorService.updateConnectorConfig(clusterId, connectorName, config));
    }

    @DeleteMapping("/{connectorName}")
    @Operation(summary = "Delete connector", description = "Delete a connector")
    public ResponseEntity<Void> deleteConnector(
            @PathVariable String clusterId,
            @PathVariable String connectorName) {
        connectorService.deleteConnector(clusterId, connectorName);
        return ResponseEntity.noContent().build();
    }

    @PostMapping("/{connectorName}/restart")
    @Operation(summary = "Restart connector", description = "Restart a connector")
    public ResponseEntity<Void> restartConnector(
            @PathVariable String clusterId,
            @PathVariable String connectorName) {
        connectorService.restartConnector(clusterId, connectorName);
        return ResponseEntity.ok().build();
    }

    @PutMapping("/{connectorName}/pause")
    @Operation(summary = "Pause connector", description = "Pause a connector")
    public ResponseEntity<Void> pauseConnector(
            @PathVariable String clusterId,
            @PathVariable String connectorName) {
        connectorService.pauseConnector(clusterId, connectorName);
        return ResponseEntity.ok().build();
    }

    @PutMapping("/{connectorName}/resume")
    @Operation(summary = "Resume connector", description = "Resume a paused connector")
    public ResponseEntity<Void> resumeConnector(
            @PathVariable String clusterId,
            @PathVariable String connectorName) {
        connectorService.resumeConnector(clusterId, connectorName);
        return ResponseEntity.ok().build();
    }

    @PostMapping("/{connectorName}/tasks/{taskId}/restart")
    @Operation(summary = "Restart task", description = "Restart a specific connector task")
    public ResponseEntity<Void> restartTask(
            @PathVariable String clusterId,
            @PathVariable String connectorName,
            @PathVariable int taskId) {
        connectorService.restartTask(clusterId, connectorName, taskId);
        return ResponseEntity.ok().build();
    }

    @GetMapping("/plugins")
    @Operation(summary = "List plugins", description = "List available connector plugins")
    public ResponseEntity<List<ConnectorPluginDTO>> listPlugins(@PathVariable String clusterId) {
        return ResponseEntity.ok(connectorService.listPlugins(clusterId));
    }

    @PutMapping("/plugins/{pluginName}/config/validate")
    @Operation(summary = "Validate config", description = "Validate connector configuration against a plugin")
    public ResponseEntity<Map<String, Object>> validateConfig(
            @PathVariable String clusterId,
            @PathVariable String pluginName,
            @RequestBody Map<String, String> config) {
        return ResponseEntity.ok(connectorService.validateConfig(clusterId, pluginName, config));
    }
}
