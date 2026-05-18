package com.kafkamanagement.application.schema;

import com.kafkamanagement.application.schema.dto.*;
import com.kafkamanagement.common.exception.BusinessException;
import com.kafkamanagement.common.exception.ResourceNotFoundException;
import com.kafkamanagement.common.security.PermissionChecker;
import com.kafkamanagement.domain.user.model.Action;
import com.kafkamanagement.domain.user.model.Resource;
import com.kafkamanagement.infrastructure.schemaregistry.SchemaRegistryClientFactory;
import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.client.SchemaMetadata;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.schemaregistry.json.JsonSchema;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.cache.annotation.CacheEvict;
import org.springframework.cache.annotation.Cacheable;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

@Slf4j
@Service
@RequiredArgsConstructor
public class SchemaService {

    private final SchemaRegistryClientFactory schemaRegistryClientFactory;
    private final PermissionChecker permissionChecker;

    /**
     * List all subjects (schemas)
     */
    @Cacheable(value = "schemas", key = "#clusterId")
    public List<String> listSubjects(String clusterId) {
        permissionChecker.checkPermission(clusterId, Resource.SCHEMA, Action.READ);
        
        try {
            SchemaRegistryClient client = schemaRegistryClientFactory.getClient(clusterId);
            Collection<String> subjects = client.getAllSubjects();
            return subjects.stream()
                    .sorted(String::compareToIgnoreCase)
                    .collect(Collectors.toList());
        } catch (IOException | RestClientException e) {
            log.error("Failed to list subjects for cluster {}", clusterId, e);
            throw new BusinessException("Failed to list schemas: " + e.getMessage());
        }
    }

    /**
     * Get latest schema for a subject
     */
    @Cacheable(value = "schema", key = "#clusterId + '-' + #subject")
    public SchemaDTO getSchema(String clusterId, String subject) {
        permissionChecker.checkPermission(clusterId, Resource.SCHEMA, Action.READ, subject);
        
        try {
            SchemaRegistryClient client = schemaRegistryClientFactory.getClient(clusterId);
            SchemaMetadata metadata = client.getLatestSchemaMetadata(subject);
            String compatibility = client.getCompatibility(subject);
            
            return SchemaDTO.builder()
                    .subject(subject)
                    .version(metadata.getVersion())
                    .id(metadata.getId())
                    .schemaType(metadata.getSchemaType())
                    .schema(metadata.getSchema())
                    .compatibility(compatibility)
                    .build();
        } catch (RestClientException e) {
            if (e.getStatus() == 404) {
                throw new ResourceNotFoundException("Schema", subject);
            }
            log.error("Failed to get schema {} for cluster {}", subject, clusterId, e);
            throw new BusinessException("Failed to get schema: " + e.getMessage());
        } catch (IOException e) {
            log.error("Failed to get schema {} for cluster {}", subject, clusterId, e);
            throw new BusinessException("Failed to get schema: " + e.getMessage());
        }
    }

    /**
     * Get specific version of a schema
     */
    public SchemaVersionDTO getSchemaVersion(String clusterId, String subject, int version) {
        permissionChecker.checkPermission(clusterId, Resource.SCHEMA, Action.READ, subject);
        
        try {
            SchemaRegistryClient client = schemaRegistryClientFactory.getClient(clusterId);
            SchemaMetadata metadata = client.getSchemaMetadata(subject, version);
            
            return SchemaVersionDTO.builder()
                    .subject(subject)
                    .version(metadata.getVersion())
                    .id(metadata.getId())
                    .schemaType(metadata.getSchemaType())
                    .schema(metadata.getSchema())
                    .build();
        } catch (RestClientException e) {
            if (e.getStatus() == 404) {
                throw new ResourceNotFoundException("Schema version", subject + ":" + version);
            }
            log.error("Failed to get schema version {} for subject {} in cluster {}", version, subject, clusterId, e);
            throw new BusinessException("Failed to get schema version: " + e.getMessage());
        } catch (IOException e) {
            log.error("Failed to get schema version {} for subject {} in cluster {}", version, subject, clusterId, e);
            throw new BusinessException("Failed to get schema version: " + e.getMessage());
        }
    }

    /**
     * List all versions of a schema
     */
    public List<Integer> listVersions(String clusterId, String subject) {
        permissionChecker.checkPermission(clusterId, Resource.SCHEMA, Action.READ, subject);
        
        try {
            SchemaRegistryClient client = schemaRegistryClientFactory.getClient(clusterId);
            return client.getAllVersions(subject);
        } catch (RestClientException e) {
            if (e.getStatus() == 404) {
                throw new ResourceNotFoundException("Schema", subject);
            }
            log.error("Failed to list versions for subject {} in cluster {}", subject, clusterId, e);
            throw new BusinessException("Failed to list schema versions: " + e.getMessage());
        } catch (IOException e) {
            log.error("Failed to list versions for subject {} in cluster {}", subject, clusterId, e);
            throw new BusinessException("Failed to list schema versions: " + e.getMessage());
        }
    }

    /**
     * Register a new schema
     */
    @CacheEvict(value = {"schemas", "schema"}, allEntries = true)
    public SchemaDTO registerSchema(String clusterId, SchemaCreateRequest request) {
        permissionChecker.checkPermission(clusterId, Resource.SCHEMA, Action.CREATE, request.getSubject());
        
        try {
            SchemaRegistryClient client = schemaRegistryClientFactory.getClient(clusterId);
            
            // Parse schema based on type
            ParsedSchema parsedSchema = parseSchema(request.getSchemaType(), request.getSchema());
            
            // Set compatibility if specified
            if (request.getCompatibility() != null && !request.getCompatibility().isBlank()) {
                client.updateCompatibility(request.getSubject(), request.getCompatibility());
            }
            
            // Register schema
            int id = client.register(request.getSubject(), parsedSchema);
            
            // Get the registered schema metadata
            SchemaMetadata metadata = client.getLatestSchemaMetadata(request.getSubject());
            String compatibility = client.getCompatibility(request.getSubject());
            
            log.info("Registered schema {} with id {} in cluster {}", request.getSubject(), id, clusterId);
            
            return SchemaDTO.builder()
                    .subject(request.getSubject())
                    .version(metadata.getVersion())
                    .id(id)
                    .schemaType(request.getSchemaType())
                    .schema(request.getSchema())
                    .compatibility(compatibility)
                    .build();
        } catch (RestClientException | IOException e) {
            log.error("Failed to register schema {} in cluster {}", request.getSubject(), clusterId, e);
            throw new BusinessException("Failed to register schema: " + e.getMessage());
        }
    }

    /**
     * Delete a schema (all versions)
     */
    @CacheEvict(value = {"schemas", "schema"}, allEntries = true)
    public void deleteSchema(String clusterId, String subject, boolean permanent) {
        permissionChecker.checkPermission(clusterId, Resource.SCHEMA, Action.DELETE, subject);
        
        try {
            SchemaRegistryClient client = schemaRegistryClientFactory.getClient(clusterId);
            
            // Soft delete first
            client.deleteSubject(subject);
            
            // Hard delete if permanent
            if (permanent) {
                client.deleteSubject(subject, true);
            }
            
            log.info("Deleted schema {} from cluster {} (permanent: {})", subject, clusterId, permanent);
        } catch (RestClientException e) {
            if (e.getStatus() == 404) {
                throw new ResourceNotFoundException("Schema", subject);
            }
            log.error("Failed to delete schema {} from cluster {}", subject, clusterId, e);
            throw new BusinessException("Failed to delete schema: " + e.getMessage());
        } catch (IOException e) {
            log.error("Failed to delete schema {} from cluster {}", subject, clusterId, e);
            throw new BusinessException("Failed to delete schema: " + e.getMessage());
        }
    }

    /**
     * Delete a specific version of a schema
     */
    @CacheEvict(value = "schema", key = "#clusterId + '-' + #subject")
    public void deleteSchemaVersion(String clusterId, String subject, int version, boolean permanent) {
        permissionChecker.checkPermission(clusterId, Resource.SCHEMA, Action.DELETE, subject);
        
        try {
            SchemaRegistryClient client = schemaRegistryClientFactory.getClient(clusterId);
            
            // Soft delete first
            client.deleteSchemaVersion(subject, String.valueOf(version));
            
            // Hard delete if permanent
            if (permanent) {
                client.deleteSchemaVersion(subject, String.valueOf(version), true);
            }
            
            log.info("Deleted schema version {} for subject {} from cluster {} (permanent: {})", 
                    version, subject, clusterId, permanent);
        } catch (RestClientException e) {
            if (e.getStatus() == 404) {
                throw new ResourceNotFoundException("Schema version", subject + ":" + version);
            }
            log.error("Failed to delete schema version {} for subject {} from cluster {}", version, subject, clusterId, e);
            throw new BusinessException("Failed to delete schema version: " + e.getMessage());
        } catch (IOException e) {
            log.error("Failed to delete schema version {} for subject {} from cluster {}", version, subject, clusterId, e);
            throw new BusinessException("Failed to delete schema version: " + e.getMessage());
        }
    }

    /**
     * Get compatibility level for a subject
     */
    public String getCompatibility(String clusterId, String subject) {
        permissionChecker.checkPermission(clusterId, Resource.SCHEMA, Action.READ, subject);
        
        try {
            SchemaRegistryClient client = schemaRegistryClientFactory.getClient(clusterId);
            return client.getCompatibility(subject);
        } catch (RestClientException e) {
            if (e.getStatus() == 404) {
                // Return global compatibility if subject-level not set
                try {
                    return schemaRegistryClientFactory.getClient(clusterId).getCompatibility(null);
                } catch (Exception ex) {
                    return "BACKWARD";  // Default
                }
            }
            log.error("Failed to get compatibility for subject {} in cluster {}", subject, clusterId, e);
            throw new BusinessException("Failed to get compatibility: " + e.getMessage());
        } catch (IOException e) {
            log.error("Failed to get compatibility for subject {} in cluster {}", subject, clusterId, e);
            throw new BusinessException("Failed to get compatibility: " + e.getMessage());
        }
    }

    /**
     * Update compatibility level for a subject
     */
    @CacheEvict(value = "schema", key = "#clusterId + '-' + #subject")
    public String updateCompatibility(String clusterId, String subject, String compatibility) {
        permissionChecker.checkPermission(clusterId, Resource.SCHEMA, Action.UPDATE, subject);
        
        try {
            SchemaRegistryClient client = schemaRegistryClientFactory.getClient(clusterId);
            String result = client.updateCompatibility(subject, compatibility);
            
            log.info("Updated compatibility for subject {} to {} in cluster {}", subject, compatibility, clusterId);
            return result;
        } catch (RestClientException | IOException e) {
            log.error("Failed to update compatibility for subject {} in cluster {}", subject, clusterId, e);
            throw new BusinessException("Failed to update compatibility: " + e.getMessage());
        }
    }

    /**
     * Test schema compatibility
     */
    public CompatibilityCheckResult testCompatibility(String clusterId, String subject, String schema, String schemaType) {
        permissionChecker.checkPermission(clusterId, Resource.SCHEMA, Action.READ, subject);
        
        try {
            SchemaRegistryClient client = schemaRegistryClientFactory.getClient(clusterId);
            ParsedSchema parsedSchema = parseSchema(schemaType, schema);
            
            boolean compatible = client.testCompatibility(subject, parsedSchema);
            
            return CompatibilityCheckResult.builder()
                    .compatible(compatible)
                    .messages(compatible ? Collections.emptyList() : List.of("Schema is not compatible with existing versions"))
                    .build();
        } catch (RestClientException | IOException e) {
            log.error("Failed to test compatibility for subject {} in cluster {}", subject, clusterId, e);
            return CompatibilityCheckResult.builder()
                    .compatible(false)
                    .messages(List.of("Compatibility check failed: " + e.getMessage()))
                    .build();
        }
    }

    // ==================== Helper Methods ====================

    private ParsedSchema parseSchema(String schemaType, String schema) {
        return switch (schemaType.toUpperCase()) {
            case "AVRO" -> new AvroSchema(schema);
            case "JSON" -> new JsonSchema(schema);
            case "PROTOBUF" -> new ProtobufSchema(schema);
            default -> throw new BusinessException("Unsupported schema type: " + schemaType);
        };
    }
}
