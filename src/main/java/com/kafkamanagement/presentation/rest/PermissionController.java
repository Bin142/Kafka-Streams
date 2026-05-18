package com.kafkamanagement.presentation.rest;

import com.kafkamanagement.application.user.PermissionService;
import com.kafkamanagement.application.user.dto.*;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@RestController
@RequestMapping("/v1/permissions")
@RequiredArgsConstructor
@Tag(name = "Permissions", description = "Permission management APIs")
public class PermissionController {

    private final PermissionService permissionService;

    @GetMapping
    @Operation(summary = "List permissions", description = "List all permissions with optional resource filter")
    public ResponseEntity<List<PermissionDTO>> listPermissions(
            @RequestParam(required = false) String resource) {
        return ResponseEntity.ok(permissionService.listPermissions(resource));
    }

    @GetMapping("/{id}")
    @Operation(summary = "Get permission", description = "Get permission by ID")
    public ResponseEntity<PermissionDTO> getPermission(@PathVariable Long id) {
        return ResponseEntity.ok(permissionService.getPermission(id));
    }

    @PostMapping
    @Operation(summary = "Create permission", description = "Create a new permission")
    public ResponseEntity<PermissionDTO> createPermission(@Valid @RequestBody PermissionCreateRequest request) {
        return ResponseEntity.status(HttpStatus.CREATED)
                .body(permissionService.createPermission(request));
    }

    @PutMapping("/{id}")
    @Operation(summary = "Update permission", description = "Update permission information")
    public ResponseEntity<PermissionDTO> updatePermission(
            @PathVariable Long id,
            @Valid @RequestBody PermissionCreateRequest request) {
        return ResponseEntity.ok(permissionService.updatePermission(id, request));
    }

    @DeleteMapping("/{id}")
    @Operation(summary = "Delete permission", description = "Delete a permission")
    public ResponseEntity<Void> deletePermission(@PathVariable Long id) {
        permissionService.deletePermission(id);
        return ResponseEntity.noContent().build();
    }
}
