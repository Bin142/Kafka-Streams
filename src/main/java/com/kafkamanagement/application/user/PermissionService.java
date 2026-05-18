package com.kafkamanagement.application.user;

import com.kafkamanagement.application.user.dto.*;
import com.kafkamanagement.common.exception.BusinessException;
import com.kafkamanagement.common.exception.ResourceNotFoundException;
import com.kafkamanagement.common.security.PermissionChecker;
import com.kafkamanagement.domain.user.entity.PermissionEntity;
import com.kafkamanagement.domain.user.model.Action;
import com.kafkamanagement.domain.user.model.Resource;
import com.kafkamanagement.infrastructure.persistence.repository.PermissionRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.List;
import java.util.stream.Collectors;

@Slf4j
@Service
@RequiredArgsConstructor
public class PermissionService {

    private final PermissionRepository permissionRepository;
    private final PermissionChecker permissionChecker;

    /**
     * List all permissions
     */
    @Transactional(readOnly = true)
    public List<PermissionDTO> listPermissions(String resourceFilter) {
        permissionChecker.checkPermission(null, Resource.ROLE, Action.READ);
        
        List<PermissionEntity> permissions;
        if (resourceFilter != null && !resourceFilter.isBlank()) {
            try {
                Resource resource = Resource.valueOf(resourceFilter.toUpperCase());
                permissions = permissionRepository.findByResource(resource);
            } catch (IllegalArgumentException e) {
                permissions = permissionRepository.findAll();
            }
        } else {
            permissions = permissionRepository.findAll();
        }
        
        return permissions.stream()
                .map(this::toDTO)
                .collect(Collectors.toList());
    }

    /**
     * Get permission by ID
     */
    @Transactional(readOnly = true)
    public PermissionDTO getPermission(Long id) {
        permissionChecker.checkPermission(null, Resource.ROLE, Action.READ);
        
        PermissionEntity permission = permissionRepository.findById(id)
                .orElseThrow(() -> new ResourceNotFoundException("Permission", id.toString()));
        return toDTO(permission);
    }

    /**
     * Create new permission
     */
    @Transactional
    public PermissionDTO createPermission(PermissionCreateRequest request) {
        permissionChecker.checkPermission(null, Resource.ROLE, Action.CREATE);
        
        // Validate resource and action
        Resource resource;
        Action action;
        try {
            resource = Resource.valueOf(request.getResource().toUpperCase());
            action = Action.valueOf(request.getAction().toUpperCase());
        } catch (IllegalArgumentException e) {
            throw new BusinessException("Invalid resource or action: " + e.getMessage());
        }
        
        PermissionEntity permission = new PermissionEntity();
        permission.setResource(resource);
        permission.setAction(action);
        permission.setResourcePattern(request.getResourcePattern());
        permission.setClusterIds(request.getClusterIds());
        permission.setDescription(request.getDescription());
        permission.setCreatedAt(Instant.now());
        
        permission = permissionRepository.save(permission);
        log.info("Created permission: {} - {}", permission.getResource(), permission.getAction());
        
        return toDTO(permission);
    }

    /**
     * Update permission
     */
    @Transactional
    public PermissionDTO updatePermission(Long id, PermissionCreateRequest request) {
        permissionChecker.checkPermission(null, Resource.ROLE, Action.UPDATE);
        
        PermissionEntity permission = permissionRepository.findById(id)
                .orElseThrow(() -> new ResourceNotFoundException("Permission", id.toString()));
        
        // Validate resource and action
        Resource resource;
        Action action;
        try {
            resource = Resource.valueOf(request.getResource().toUpperCase());
            action = Action.valueOf(request.getAction().toUpperCase());
        } catch (IllegalArgumentException e) {
            throw new BusinessException("Invalid resource or action: " + e.getMessage());
        }
        
        permission.setResource(resource);
        permission.setAction(action);
        permission.setResourcePattern(request.getResourcePattern());
        permission.setClusterIds(request.getClusterIds());
        permission.setDescription(request.getDescription());
        
        permission = permissionRepository.save(permission);
        log.info("Updated permission: {} - {}", permission.getResource(), permission.getAction());
        
        return toDTO(permission);
    }

    /**
     * Delete permission
     */
    @Transactional
    public void deletePermission(Long id) {
        permissionChecker.checkPermission(null, Resource.ROLE, Action.DELETE);
        
        PermissionEntity permission = permissionRepository.findById(id)
                .orElseThrow(() -> new ResourceNotFoundException("Permission", id.toString()));
        
        // Check if permission is assigned to any roles
        if (!permission.getRoles().isEmpty()) {
            throw new BusinessException("Cannot delete permission that is assigned to roles. " +
                    "Remove it from roles first.");
        }
        
        permissionRepository.delete(permission);
        log.info("Deleted permission: {} - {}", permission.getResource(), permission.getAction());
    }

    // ==================== Helper Methods ====================

    private PermissionDTO toDTO(PermissionEntity permission) {
        return PermissionDTO.builder()
                .id(permission.getId())
                .resource(permission.getResource().name())
                .action(permission.getAction().name())
                .resourcePattern(permission.getResourcePattern())
                .clusterIds(permission.getClusterIds())
                .description(permission.getDescription())
                .createdAt(permission.getCreatedAt())
                .build();
    }
}
