package com.kafkamanagement.presentation.rest;

import com.kafkamanagement.application.schema.SchemaService;
import com.kafkamanagement.application.schema.dto.*;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@RestController
@RequestMapping("/v1/clusters/{clusterId}/schemas")
@RequiredArgsConstructor
@Tag(name = "Schema Registry", description = "Schema Registry management APIs")
public class SchemaController {

    private final SchemaService schemaService;

    @GetMapping
    @Operation(summary = "List schemas", description = "List all subjects (schemas) in the Schema Registry")
    public ResponseEntity<List<String>> listSubjects(@PathVariable String clusterId) {
        return ResponseEntity.ok(schemaService.listSubjects(clusterId));
    }

    @PostMapping
    @Operation(summary = "Register schema", description = "Register a new schema or new version of existing schema")
    public ResponseEntity<SchemaDTO> registerSchema(
            @PathVariable String clusterId,
            @Valid @RequestBody SchemaCreateRequest request) {
        return ResponseEntity.status(HttpStatus.CREATED)
                .body(schemaService.registerSchema(clusterId, request));
    }

    @GetMapping("/{subject}")
    @Operation(summary = "Get schema", description = "Get the latest version of a schema")
    public ResponseEntity<SchemaDTO> getSchema(
            @PathVariable String clusterId,
            @PathVariable String subject) {
        return ResponseEntity.ok(schemaService.getSchema(clusterId, subject));
    }

    @DeleteMapping("/{subject}")
    @Operation(summary = "Delete schema", description = "Delete all versions of a schema")
    public ResponseEntity<Void> deleteSchema(
            @PathVariable String clusterId,
            @PathVariable String subject,
            @RequestParam(defaultValue = "false") boolean permanent) {
        schemaService.deleteSchema(clusterId, subject, permanent);
        return ResponseEntity.noContent().build();
    }

    @GetMapping("/{subject}/versions")
    @Operation(summary = "List versions", description = "List all versions of a schema")
    public ResponseEntity<List<Integer>> listVersions(
            @PathVariable String clusterId,
            @PathVariable String subject) {
        return ResponseEntity.ok(schemaService.listVersions(clusterId, subject));
    }

    @GetMapping("/{subject}/versions/{version}")
    @Operation(summary = "Get schema version", description = "Get a specific version of a schema")
    public ResponseEntity<SchemaVersionDTO> getSchemaVersion(
            @PathVariable String clusterId,
            @PathVariable String subject,
            @PathVariable int version) {
        return ResponseEntity.ok(schemaService.getSchemaVersion(clusterId, subject, version));
    }

    @DeleteMapping("/{subject}/versions/{version}")
    @Operation(summary = "Delete schema version", description = "Delete a specific version of a schema")
    public ResponseEntity<Void> deleteSchemaVersion(
            @PathVariable String clusterId,
            @PathVariable String subject,
            @PathVariable int version,
            @RequestParam(defaultValue = "false") boolean permanent) {
        schemaService.deleteSchemaVersion(clusterId, subject, version, permanent);
        return ResponseEntity.noContent().build();
    }

    @GetMapping("/{subject}/config")
    @Operation(summary = "Get compatibility", description = "Get compatibility level for a subject")
    public ResponseEntity<String> getCompatibility(
            @PathVariable String clusterId,
            @PathVariable String subject) {
        return ResponseEntity.ok(schemaService.getCompatibility(clusterId, subject));
    }

    @PutMapping("/{subject}/config")
    @Operation(summary = "Update compatibility", description = "Update compatibility level for a subject")
    public ResponseEntity<String> updateCompatibility(
            @PathVariable String clusterId,
            @PathVariable String subject,
            @RequestParam String compatibility) {
        return ResponseEntity.ok(schemaService.updateCompatibility(clusterId, subject, compatibility));
    }

    @PostMapping("/{subject}/compatibility")
    @Operation(summary = "Test compatibility", description = "Test if a schema is compatible with existing versions")
    public ResponseEntity<CompatibilityCheckResult> testCompatibility(
            @PathVariable String clusterId,
            @PathVariable String subject,
            @RequestParam(defaultValue = "AVRO") String schemaType,
            @RequestBody String schema) {
        return ResponseEntity.ok(schemaService.testCompatibility(clusterId, subject, schema, schemaType));
    }
}
