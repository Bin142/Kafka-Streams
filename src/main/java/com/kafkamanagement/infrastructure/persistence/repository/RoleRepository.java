package com.kafkamanagement.infrastructure.persistence.repository;

import com.kafkamanagement.domain.user.entity.RoleEntity;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.Optional;

@Repository
public interface RoleRepository extends JpaRepository<RoleEntity, Long> {

    Optional<RoleEntity> findByName(String name);

    boolean existsByName(String name);

    Page<RoleEntity> findByNameContainingIgnoreCase(String name, Pageable pageable);

    Page<RoleEntity> findByIsSystem(Boolean isSystem, Pageable pageable);
}
