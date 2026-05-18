package com.kafkamanagement.application.acl;

import com.kafkamanagement.application.acl.dto.AclCreateRequest;
import com.kafkamanagement.application.acl.dto.AclDTO;
import com.kafkamanagement.application.acl.dto.AclDeleteRequest;
import com.kafkamanagement.common.exception.BusinessException;
import com.kafkamanagement.common.security.PermissionChecker;
import com.kafkamanagement.domain.user.model.Action;
import com.kafkamanagement.domain.user.model.Resource;
import com.kafkamanagement.infrastructure.kafka.KafkaAdminWrapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.acl.*;
import org.apache.kafka.common.resource.PatternType;
import org.apache.kafka.common.resource.ResourcePattern;
import org.apache.kafka.common.resource.ResourcePatternFilter;
import org.apache.kafka.common.resource.ResourceType;
import org.springframework.stereotype.Service;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

@Slf4j
@Service
@RequiredArgsConstructor
public class AclService {

    private final KafkaAdminWrapper kafkaAdminWrapper;
    private final PermissionChecker permissionChecker;

    /**
     * List all ACLs in the cluster
     */
    public List<AclDTO> listAcls(String clusterId, String resourceType, String principal, String operation) {
        permissionChecker.checkPermission(clusterId, Resource.ACL, Action.READ);
        
        try {
            // Build filter
            AclBindingFilter filter = buildAclFilter(resourceType, null, null, principal, null, operation, null);
            
            Collection<AclBinding> acls = kafkaAdminWrapper.describeAcls(clusterId, filter);
            
            return acls.stream()
                    .map(this::toAclDTO)
                    .sorted((a, b) -> {
                        int cmp = a.getResourceType().compareTo(b.getResourceType());
                        if (cmp != 0) return cmp;
                        cmp = a.getResourceName().compareTo(b.getResourceName());
                        if (cmp != 0) return cmp;
                        return a.getPrincipal().compareTo(b.getPrincipal());
                    })
                    .collect(Collectors.toList());
            
        } catch (ExecutionException | InterruptedException e) {
            log.error("Failed to list ACLs for cluster {}", clusterId, e);
            throw new BusinessException("Failed to list ACLs: " + e.getMessage());
        }
    }

    /**
     * Create a new ACL
     */
    public void createAcl(String clusterId, AclCreateRequest request) {
        permissionChecker.checkPermission(clusterId, Resource.ACL, Action.CREATE);
        
        try {
            AclBinding aclBinding = buildAclBinding(request);
            kafkaAdminWrapper.createAcls(clusterId, Collections.singleton(aclBinding));
            
            log.info("Created ACL in cluster {}: {} {} on {} {} for {} from {}", 
                    clusterId,
                    request.getPermissionType(),
                    request.getOperation(),
                    request.getResourceType(),
                    request.getResourceName(),
                    request.getPrincipal(),
                    request.getHost());
            
        } catch (ExecutionException | InterruptedException e) {
            log.error("Failed to create ACL in cluster {}", clusterId, e);
            throw new BusinessException("Failed to create ACL: " + e.getMessage());
        }
    }

    /**
     * Delete ACLs matching the filter
     */
    public int deleteAcls(String clusterId, AclDeleteRequest request) {
        permissionChecker.checkPermission(clusterId, Resource.ACL, Action.DELETE);
        
        try {
            AclBindingFilter filter = buildAclFilter(
                    request.getResourceType(),
                    request.getResourceName(),
                    request.getPatternType(),
                    request.getPrincipal(),
                    request.getHost(),
                    request.getOperation(),
                    request.getPermissionType()
            );
            
            // First, find matching ACLs
            Collection<AclBinding> matchingAcls = kafkaAdminWrapper.describeAcls(clusterId, filter);
            int count = matchingAcls.size();
            
            if (count > 0) {
                kafkaAdminWrapper.deleteAcls(clusterId, Collections.singleton(filter));
                log.info("Deleted {} ACLs from cluster {}", count, clusterId);
            }
            
            return count;
            
        } catch (ExecutionException | InterruptedException e) {
            log.error("Failed to delete ACLs from cluster {}", clusterId, e);
            throw new BusinessException("Failed to delete ACLs: " + e.getMessage());
        }
    }

    // ==================== Helper Methods ====================

    private AclDTO toAclDTO(AclBinding binding) {
        return AclDTO.builder()
                .resourceType(binding.pattern().resourceType().name())
                .resourceName(binding.pattern().name())
                .patternType(binding.pattern().patternType().name())
                .principal(binding.entry().principal())
                .host(binding.entry().host())
                .operation(binding.entry().operation().name())
                .permissionType(binding.entry().permissionType().name())
                .build();
    }

    private AclBinding buildAclBinding(AclCreateRequest request) {
        ResourceType resourceType = ResourceType.valueOf(request.getResourceType().toUpperCase());
        PatternType patternType = PatternType.valueOf(request.getPatternType().toUpperCase());
        AclOperation operation = AclOperation.valueOf(request.getOperation().toUpperCase());
        AclPermissionType permissionType = AclPermissionType.valueOf(request.getPermissionType().toUpperCase());
        
        ResourcePattern pattern = new ResourcePattern(resourceType, request.getResourceName(), patternType);
        AccessControlEntry entry = new AccessControlEntry(
                request.getPrincipal(),
                request.getHost(),
                operation,
                permissionType
        );
        
        return new AclBinding(pattern, entry);
    }

    private AclBindingFilter buildAclFilter(String resourceType, String resourceName, String patternType,
                                            String principal, String host, String operation, String permissionType) {
        ResourceType resType = resourceType != null ? 
                ResourceType.valueOf(resourceType.toUpperCase()) : ResourceType.ANY;
        PatternType patType = patternType != null ? 
                PatternType.valueOf(patternType.toUpperCase()) : PatternType.ANY;
        AclOperation op = operation != null ? 
                AclOperation.valueOf(operation.toUpperCase()) : AclOperation.ANY;
        AclPermissionType permType = permissionType != null ? 
                AclPermissionType.valueOf(permissionType.toUpperCase()) : AclPermissionType.ANY;
        
        ResourcePatternFilter patternFilter = new ResourcePatternFilter(
                resType,
                resourceName,
                patType
        );
        
        AccessControlEntryFilter entryFilter = new AccessControlEntryFilter(
                principal,
                host,
                op,
                permType
        );
        
        return new AclBindingFilter(patternFilter, entryFilter);
    }
}
