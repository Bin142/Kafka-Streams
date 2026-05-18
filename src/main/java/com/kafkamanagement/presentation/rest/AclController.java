package com.kafkamanagement.presentation.rest;

import com.kafkamanagement.application.acl.AclService;
import com.kafkamanagement.application.acl.dto.AclCreateRequest;
import com.kafkamanagement.application.acl.dto.AclDTO;
import com.kafkamanagement.application.acl.dto.AclDeleteRequest;
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
@RequestMapping("/v1/clusters/{clusterId}/acls")
@RequiredArgsConstructor
@Tag(name = "ACLs", description = "Kafka ACL management APIs")
public class AclController {

    private final AclService aclService;

    @GetMapping
    @Operation(summary = "List ACLs with optional filtering")
    public ResponseEntity<List<AclDTO>> listAcls(
            @PathVariable String clusterId,
            @RequestParam(required = false) String resourceType,
            @RequestParam(required = false) String principal,
            @RequestParam(required = false) String operation) {
        return ResponseEntity.ok(aclService.listAcls(clusterId, resourceType, principal, operation));
    }

    @PostMapping
    @Operation(summary = "Create a new ACL")
    public ResponseEntity<Void> createAcl(
            @PathVariable String clusterId,
            @Valid @RequestBody AclCreateRequest request) {
        aclService.createAcl(clusterId, request);
        return ResponseEntity.status(HttpStatus.CREATED).build();
    }

    @DeleteMapping
    @Operation(summary = "Delete ACLs matching the filter")
    public ResponseEntity<Map<String, Integer>> deleteAcls(
            @PathVariable String clusterId,
            @RequestBody AclDeleteRequest request) {
        int deletedCount = aclService.deleteAcls(clusterId, request);
        return ResponseEntity.ok(Map.of("deletedCount", deletedCount));
    }
}
