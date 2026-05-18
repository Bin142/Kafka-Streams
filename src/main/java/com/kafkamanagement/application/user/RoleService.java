package com.kafkamanagement.application.user;

import com.kafkamanagement.application.user.dto.*;
import com.kafkamanagement.common.exception.BusinessException;
import com.kafkamanagement.common.exception.ResourceNotFoundException;
import com.kafkamanagement.common.security.PermissionChecker;
import com.kafkamanagement.domain.user.entity.PermissionEntity;
import com.kafkamanagement.domain.user.entity.RoleEntity;
import com.kafkamanagement.domain.user.model.Action;
import com.kafkamanagement.domain.user.model.Resource;
import com.kafkamanagement.infrastructure.persistence.repository.PermissionRepository;
import com.kafkamanagement.infrastructure.persistence.repository.RoleRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

@Slf4j
@Service
@RequiredArgsConstructor
public class RoleService {

    private final RoleRepository roleRepository;
    private final PermissionRepository permissionRepository;
    private final PermissionChecker permissionChecker;

    /**
     * List all roles
     */
    @Transactional(readOnly = true)
    public List<RoleDTO> listRoles() {
        permissionChecker.checkPermission(null, Resource.ROLE, Action.READ);
        
        return roleRepository.findAll().stream()
                .map(this::toDTO)
                .collect(Collectors.toList());
    }

    /**
     * Get role by ID
     */
    @Transactional(readOnly = true)
    public RoleDTO getRole(Long id) {
        permissionChecker.checkPermission(null, Resource.ROLE, Action.READ);
        
        RoleEntity role = roleRepository.findById(id)
                .orElseThrow(() -> new ResourceNotFoundException("Role", id.toString()));
        return toDTO(role);
    }

    /**
     * Create new role
     */
    @Transactional
    public RoleDTO createRole(RoleCreateRequest request) {
        permissionChecker.checkPermission(null, Resource.ROLE, Action.CREATE);
        
        // Check if role name already exists
        if (roleRepository.existsByName(request.getName())) {
            throw new BusinessException("Role name already exists: " + request.getName());
        }
        
        RoleEntity role = new RoleEntity();
        role.setName(request.getName());
        role.setDescription(request.getDescription());
        role.setSystem(false);
        role.setCreatedAt(Instant.now());
        role.setUpdatedAt(Instant.now());
        
        // Assign permissions
        if (request.getPermissionIds() != null && !request.getPermissionIds().isEmpty()) {
            Set<PermissionEntity> permissions = new HashSet<>(permissionRepository.findAllById(request.getPermissionIds()));
            role.setPermissions(permissions);
        }
        
        role = roleRepository.save(role);
        log.info("Created role: {}", role.getName());
        
        return toDTO(role);
    }

    /**
     * Update role
     */
    @Transactional
    public RoleDTO updateRole(Long id, RoleCreateRequest request) {
        permissionChecker.checkPermission(null, Resource.ROLE, Action.UPDATE);
        
        RoleEntity role = roleRepository.findById(id)
                .orElseThrow(() -> new ResourceNotFoundException("Role", id.toString()));
        
        // Cannot update system roles name
        if (role.isSystem() && !role.getName().equals(request.getName())) {
            throw new BusinessException("Cannot change name of system role: " + role.getName());
        }
        
        // Check if new name already exists for another role
        if (!role.getName().equals(request.getName()) && roleRepository.existsByName(request.getName())) {
            throw new BusinessException("Role name already exists: " + request.getName());
        }
        
        role.setName(request.getName());
        role.setDescription(request.getDescription());
        role.setUpdatedAt(Instant.now());
        
        // Update permissions
        if (request.getPermissionIds() != null) {
            Set<PermissionEntity> permissions = new HashSet<>(permissionRepository.findAllById(request.getPermissionIds()));
            role.setPermissions(permissions);
        }
        
        role = roleRepository.save(role);
        log.info("Updated role: {}", role.getName());
        
        return toDTO(role);
    }

    /**
     * Delete role
     */
    @Transactional
    public void deleteRole(Long id) {
        permissionChecker.checkPermission(null, Resource.ROLE, Action.DELETE);
        
        RoleEntity role = roleRepository.findById(id)
                .orElseThrow(() -> new ResourceNotFoundException("Role", id.toString()));
        
        // Cannot delete system roles
        if (role.isSystem()) {
            throw new BusinessException("Cannot delete system role: " + role.getName());
        }
        
        roleRepository.delete(role);
        log.info("Deleted role: {}", role.getName());
    }

    /**
     * Assign permissions to role
     */
    @Transactional
    public RoleDTO assignPermissions(Long roleId, Set<Long> permissionIds) {
        permissionChecker.checkPermission(null, Resource.ROLE, Action.UPDATE);
        
        RoleEntity role = roleRepository.findById(roleId)
                .orElseThrow(() -> new ResourceNotFoundException("Role", roleId.toString()));
        
        Set<PermissionEntity> permissions = new HashSet<>(permissionRepository.findAllById(permissionIds));
        role.setPermissions(permissions);
        role.setUpdatedAt(Instant.now());
        role = roleRepository.save(role);
        
        log.info("Assigned {} permissions to role: {}", permissions.size(), role.getName());
        return toDTO(role);
    }

    // ==================== Helper Methods ====================

    private RoleDTO toDTO(RoleEntity role) {
        return RoleDTO.builder()
                .id(role.getId())
                .name(role.getName())
                .description(role.getDescription())
                .isSystem(role.isSystem())
                .permissions(role.getPermissions().stream()
                        .map(perm -> PermissionDTO.builder()
                                .id(perm.getId())
                                .resource(perm.getResource().name())
                                .action(perm.getAction().name())
                                .resourcePattern(perm.getResourcePattern())
                                .clusterIds(perm.getClusterIds())
                                .description(perm.getDescription())
                                .createdAt(perm.getCreatedAt())
                                .build())
                        .collect(Collectors.toSet()))
                .createdAt(role.getCreatedAt())
                .updatedAt(role.getUpdatedAt())
                .build();
    }
}
