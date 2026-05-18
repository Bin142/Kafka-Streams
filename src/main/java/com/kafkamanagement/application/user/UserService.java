package com.kafkamanagement.application.user;

import com.kafkamanagement.application.user.dto.*;
import com.kafkamanagement.common.exception.BusinessException;
import com.kafkamanagement.common.exception.ResourceNotFoundException;
import com.kafkamanagement.common.security.PermissionChecker;
import com.kafkamanagement.domain.user.entity.RoleEntity;
import com.kafkamanagement.domain.user.entity.UserEntity;
import com.kafkamanagement.domain.user.model.Action;
import com.kafkamanagement.domain.user.model.Resource;
import com.kafkamanagement.infrastructure.persistence.repository.RoleRepository;
import com.kafkamanagement.infrastructure.persistence.repository.UserRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.security.crypto.password.PasswordEncoder;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;

@Slf4j
@Service
@RequiredArgsConstructor
public class UserService {

    private final UserRepository userRepository;
    private final RoleRepository roleRepository;
    private final PasswordEncoder passwordEncoder;
    private final PermissionChecker permissionChecker;

    /**
     * List all users with pagination
     */
    @Transactional(readOnly = true)
    public Page<UserDTO> listUsers(Pageable pageable, String search) {
        permissionChecker.checkPermission(null, Resource.USER, Action.READ);
        
        Page<UserEntity> users;
        if (search != null && !search.isBlank()) {
            users = userRepository.findByUsernameContainingIgnoreCaseOrEmailContainingIgnoreCase(
                    search, search, pageable);
        } else {
            users = userRepository.findAll(pageable);
        }
        
        return users.map(this::toDTO);
    }

    /**
     * Get user by ID
     */
    @Transactional(readOnly = true)
    public UserDTO getUser(Long id) {
        permissionChecker.checkPermission(null, Resource.USER, Action.READ);
        
        UserEntity user = userRepository.findById(id)
                .orElseThrow(() -> new ResourceNotFoundException("User", id.toString()));
        return toDTO(user);
    }

    /**
     * Create new user
     */
    @Transactional
    public UserDTO createUser(UserCreateRequest request) {
        permissionChecker.checkPermission(null, Resource.USER, Action.CREATE);
        
        // Check if username already exists
        if (userRepository.existsByUsername(request.getUsername())) {
            throw new BusinessException("Username already exists: " + request.getUsername());
        }
        
        // Check if email already exists
        if (userRepository.existsByEmail(request.getEmail())) {
            throw new BusinessException("Email already exists: " + request.getEmail());
        }
        
        UserEntity user = new UserEntity();
        user.setUsername(request.getUsername());
        user.setEmail(request.getEmail());
        user.setPasswordHash(passwordEncoder.encode(request.getPassword()));
        user.setFullName(request.getFullName());
        user.setAdmin(request.isAdmin());
        user.setActive(true);
        user.setCreatedAt(Instant.now());
        user.setUpdatedAt(Instant.now());
        
        // Assign roles
        if (request.getRoleIds() != null && !request.getRoleIds().isEmpty()) {
            Set<RoleEntity> roles = new HashSet<>(roleRepository.findAllById(request.getRoleIds()));
            user.setRoles(roles);
        }
        
        user = userRepository.save(user);
        log.info("Created user: {}", user.getUsername());
        
        return toDTO(user);
    }

    /**
     * Update user
     */
    @Transactional
    public UserDTO updateUser(Long id, UserUpdateRequest request) {
        permissionChecker.checkPermission(null, Resource.USER, Action.UPDATE);
        
        UserEntity user = userRepository.findById(id)
                .orElseThrow(() -> new ResourceNotFoundException("User", id.toString()));
        
        if (request.getEmail() != null) {
            // Check if email already exists for another user
            if (userRepository.existsByEmailAndIdNot(request.getEmail(), id)) {
                throw new BusinessException("Email already exists: " + request.getEmail());
            }
            user.setEmail(request.getEmail());
        }
        
        if (request.getFullName() != null) {
            user.setFullName(request.getFullName());
        }
        
        if (request.getPassword() != null) {
            user.setPasswordHash(passwordEncoder.encode(request.getPassword()));
        }
        
        if (request.getIsActive() != null) {
            user.setActive(request.getIsActive());
        }
        
        user.setUpdatedAt(Instant.now());
        user = userRepository.save(user);
        
        log.info("Updated user: {}", user.getUsername());
        return toDTO(user);
    }

    /**
     * Delete user (soft delete)
     */
    @Transactional
    public void deleteUser(Long id) {
        permissionChecker.checkPermission(null, Resource.USER, Action.DELETE);
        
        UserEntity user = userRepository.findById(id)
                .orElseThrow(() -> new ResourceNotFoundException("User", id.toString()));
        
        // Cannot delete admin user
        if (user.isAdmin() && user.getUsername().equals("admin")) {
            throw new BusinessException("Cannot delete the default admin user");
        }
        
        user.setActive(false);
        user.setUpdatedAt(Instant.now());
        userRepository.save(user);
        
        log.info("Deactivated user: {}", user.getUsername());
    }

    /**
     * Assign roles to user
     */
    @Transactional
    public UserDTO assignRoles(Long userId, Set<Long> roleIds) {
        permissionChecker.checkPermission(null, Resource.USER, Action.UPDATE);
        
        UserEntity user = userRepository.findById(userId)
                .orElseThrow(() -> new ResourceNotFoundException("User", userId.toString()));
        
        Set<RoleEntity> roles = new HashSet<>(roleRepository.findAllById(roleIds));
        user.setRoles(roles);
        user.setUpdatedAt(Instant.now());
        user = userRepository.save(user);
        
        log.info("Assigned {} roles to user: {}", roles.size(), user.getUsername());
        return toDTO(user);
    }

    /**
     * Get user's effective permissions
     */
    @Transactional(readOnly = true)
    public Set<PermissionDTO> getUserPermissions(Long userId) {
        permissionChecker.checkPermission(null, Resource.USER, Action.READ);
        
        UserEntity user = userRepository.findById(userId)
                .orElseThrow(() -> new ResourceNotFoundException("User", userId.toString()));
        
        return user.getRoles().stream()
                .flatMap(role -> role.getPermissions().stream())
                .map(perm -> PermissionDTO.builder()
                        .id(perm.getId())
                        .resource(perm.getResource().name())
                        .action(perm.getAction().name())
                        .resourcePattern(perm.getResourcePattern())
                        .clusterIds(perm.getClusterIds())
                        .description(perm.getDescription())
                        .createdAt(perm.getCreatedAt())
                        .build())
                .collect(Collectors.toSet());
    }

    // ==================== Helper Methods ====================

    private UserDTO toDTO(UserEntity user) {
        return UserDTO.builder()
                .id(user.getId())
                .username(user.getUsername())
                .email(user.getEmail())
                .fullName(user.getFullName())
                .isAdmin(user.isAdmin())
                .isActive(user.isActive())
                .roles(user.getRoles().stream()
                        .map(RoleEntity::getName)
                        .collect(Collectors.toSet()))
                .createdAt(user.getCreatedAt())
                .updatedAt(user.getUpdatedAt())
                .build();
    }
}
