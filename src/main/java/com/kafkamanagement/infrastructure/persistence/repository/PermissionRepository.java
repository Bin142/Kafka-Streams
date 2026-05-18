package com.kafkamanagement.infrastructure.persistence.repository;

import com.kafkamanagement.domain.user.entity.PermissionEntity;
import com.kafkamanagement.domain.user.model.Action;
import com.kafkamanagement.domain.user.model.Resource;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.util.List;

@Repository
public interface PermissionRepository extends JpaRepository<PermissionEntity, Long> {

    List<PermissionEntity> findByResource(Resource resource);

    Page<PermissionEntity> findByResource(Resource resource, Pageable pageable);

    List<PermissionEntity> findByResourceAndAction(Resource resource, Action action);

    @Query("SELECT p FROM PermissionEntity p WHERE " +
            "(:resource IS NULL OR p.resource = :resource) AND " +
            "(:action IS NULL OR p.action = :action)")
    Page<PermissionEntity> findByFilters(
            @Param("resource") Resource resource,
            @Param("action") Action action,
            Pageable pageable);

    @Query("SELECT DISTINCT p FROM PermissionEntity p " +
            "JOIN p.clusterIds c WHERE c = :clusterId")
    List<PermissionEntity> findByClusterId(@Param("clusterId") String clusterId);
}
