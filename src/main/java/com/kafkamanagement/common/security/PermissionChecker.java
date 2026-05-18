package com.kafkamanagement.common.security;

import com.kafkamanagement.common.exception.ForbiddenException;
import com.kafkamanagement.domain.user.entity.PermissionEntity;
import com.kafkamanagement.domain.user.model.Action;
import com.kafkamanagement.domain.user.model.Resource;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.stereotype.Component;

import java.util.Set;

@Slf4j
@Component
public class PermissionChecker {

    @Value("${security.auth.disabled:false}")
    private boolean authDisabled;

    /**
     * Check if current user has permission (throws exception if not)
     */
    public void checkPermission(String clusterId, Resource resource, Action action) {
        checkPermission(clusterId, resource, action, null);
    }

    /**
     * Check if current user has permission for specific resource (throws exception if not)
     */
    public void checkPermission(String clusterId, Resource resource, Action action, String resourceName) {
        // Bypass permission check if auth is disabled
        if (authDisabled) {
            return;
        }

        UserPrincipal user = getCurrentUser();
        if (!hasPermission(user, clusterId, resource, action, resourceName)) {
            log.warn("User {} does not have {} permission on {} (cluster: {}, resource: {})",
                    user.getUsername(), action, resource, clusterId, resourceName);
            throw new ForbiddenException(
                    String.format("You don't have %s permission on %s", action.getValue(), resource.getValue())
            );
        }
    }

    /**
     * Check if current user has permission (returns boolean)
     */
    public boolean hasPermission(String clusterId, Resource resource, Action action) {
        return hasPermission(clusterId, resource, action, null);
    }

    /**
     * Check if current user has permission for specific resource (returns boolean)
     */
    public boolean hasPermission(String clusterId, Resource resource, Action action, String resourceName) {
        // Bypass permission check if auth is disabled
        if (authDisabled) {
            return true;
        }

        UserPrincipal user = getCurrentUser();
        return hasPermission(user, clusterId, resource, action, resourceName);
    }

    /**
     * Check if user has permission
     */
    public boolean hasPermission(UserPrincipal user, String clusterId, Resource resource,
                                  Action action, String resourceName) {
        // Admin has all permissions
        if (user.isAdmin()) {
            return true;
        }

        // Check user's permissions
        Set<PermissionEntity> permissions = user.getPermissions();
        return permissions.stream()
                .anyMatch(permission -> permission.matches(clusterId, resource, action, resourceName));
    }

    /**
     * Check if current user can access cluster
     */
    public boolean canAccessCluster(String clusterId) {
        return hasPermission(clusterId, Resource.CLUSTER, Action.READ);
    }

    /**
     * Get current authenticated user
     */
    public UserPrincipal getCurrentUser() {
        // Return null/mock user if auth is disabled
        if (authDisabled) {
            return null;
        }

        Authentication authentication = SecurityContextHolder.getContext().getAuthentication();
        if (authentication == null || !authentication.isAuthenticated()) {
            throw new ForbiddenException("User not authenticated");
        }
        if (!(authentication.getPrincipal() instanceof UserPrincipal)) {
            throw new ForbiddenException("Invalid authentication principal");
        }
        return (UserPrincipal) authentication.getPrincipal();
    }

    /**
     * Get current user ID
     */
    public Long getCurrentUserId() {
        if (authDisabled) {
            return 1L; // Default admin user
        }
        return getCurrentUser().getId();
    }

    /**
     * Get current username
     */
    public String getCurrentUsername() {
        if (authDisabled) {
            return "admin";
        }
        return getCurrentUser().getUsername();
    }

    /**
     * Check if current user is admin
     */
    public boolean isCurrentUserAdmin() {
        if (authDisabled) {
            return true;
        }
        return getCurrentUser().isAdmin();
    }
}
