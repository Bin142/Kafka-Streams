package com.kafkamanagement.presentation.rest;

import com.kafkamanagement.application.cluster.ClusterService;
import com.kafkamanagement.application.cluster.dto.ClusterDTO;
import com.kafkamanagement.application.cluster.dto.ClusterDetailDTO;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@RestController
@RequestMapping("/v1/clusters")
@RequiredArgsConstructor
@Tag(name = "Clusters", description = "Cluster management APIs")
public class ClusterController {

    private final ClusterService clusterService;

    @GetMapping
    @Operation(summary = "List all clusters user has access to")
    public ResponseEntity<List<ClusterDTO>> listClusters() {
        return ResponseEntity.ok(clusterService.listClusters());
    }

    @GetMapping("/{clusterId}")
    @Operation(summary = "Get cluster details")
    public ResponseEntity<ClusterDetailDTO> getCluster(@PathVariable String clusterId) {
        return ResponseEntity.ok(clusterService.getCluster(clusterId));
    }

    @GetMapping("/{clusterId}/nodes")
    @Operation(summary = "Get cluster nodes")
    public ResponseEntity<List<ClusterDetailDTO.NodeDTO>> getClusterNodes(@PathVariable String clusterId) {
        return ResponseEntity.ok(clusterService.getClusterNodes(clusterId));
    }
}
