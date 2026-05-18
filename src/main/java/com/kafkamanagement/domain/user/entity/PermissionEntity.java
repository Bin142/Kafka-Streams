package com.kafkamanagement.domain.user.entity;

import com.kafkamanagement.domain.user.model.Action;
import com.kafkamanagement.domain.user.model.Resource;
import jakarta.persistence.*;
import lombok.*;

import java.time.Instant;
import java.util.HashSet;
import java.util.Set;

@Entity
@Table(name = "permissions")
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class PermissionEntity {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false, length = 50)
    private Resource resource;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false, length = 50)
    private Action action;

    @Column(name = "resource_pattern", length = 255)
    private String resourcePattern;

    @Column(length = 255)
    private String description;

    @Column(name = "created_at")
    @Builder.Default
    private Instant createdAt = Instant.now();

    @ElementCollection(fetch = FetchType.EAGER)
    @CollectionTable(
            name = "permission_clusters",
            joinColumns = @JoinColumn(name = "permission_id")
    )
    @Column(name = "cluster_id")
    @Builder.Default
    private Set<String> clusterIds = new HashSet<>();

    @ManyToMany(mappedBy = "permissions", fetch = FetchType.LAZY)
    @Builder.Default
    private Set<RoleEntity> roles = new HashSet<>();

    /**
     * Check if this permission matches the given criteria
     */
    public boolean matches(String clusterId, Resource resource, Action action, String resourceName) {
        // Check resource type
        if (this.resource != resource) {
            return false;
        }

        // Check action (MANAGE includes all actions)
        if (!this.action.includes(action)) {
            return false;
        }

        // Check cluster (empty = all clusters)
        if (!clusterIds.isEmpty() && clusterId != null && !clusterIds.contains(clusterId)) {
            return false;
        }

        // Check resource pattern (null = all resources)
        if (resourcePattern != null && resourceName != null) {
            return matchesPattern(resourceName, resourcePattern);
        }

        return true;
    }

    private boolean matchesPattern(String name, String pattern) {
        // Convert glob pattern to regex
        // "dev-*" -> "dev-.*"
        // "orders-?" -> "orders-."
        String regex = pattern
                .replace(".", "\\.")
                .replace("*", ".*")
                .replace("?", ".");
        return name.matches(regex);
    }

    /**
     * Generate display description for UI
     */
    public String getDisplayDescription() {
        StringBuilder sb = new StringBuilder();
        sb.append(action.getValue().toUpperCase())
                .append(" ")
                .append(resource.getValue());

        if (resourcePattern != null) {
            sb.append(" matching '").append(resourcePattern).append("'");
        }

        if (!clusterIds.isEmpty()) {
            sb.append(" on clusters: ").append(String.join(", ", clusterIds));
        }

        return sb.toString();
    }
}
