package com.kafkamanagement.infrastructure.persistence.repository;

import com.kafkamanagement.domain.user.entity.UserEntity;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.util.Optional;

@Repository
public interface UserRepository extends JpaRepository<UserEntity, Long> {

    Optional<UserEntity> findByUsername(String username);

    Optional<UserEntity> findByEmail(String email);

    boolean existsByUsername(String username);

    boolean existsByEmail(String email);

    boolean existsByEmailAndIdNot(String email, Long id);

    Page<UserEntity> findByUsernameContainingIgnoreCaseOrEmailContainingIgnoreCase(
            String username, String email, Pageable pageable);

    @Query("SELECT u FROM UserEntity u WHERE " +
            "(:search IS NULL OR LOWER(u.username) LIKE LOWER(CONCAT('%', :search, '%')) " +
            "OR LOWER(u.email) LIKE LOWER(CONCAT('%', :search, '%')) " +
            "OR LOWER(u.fullName) LIKE LOWER(CONCAT('%', :search, '%')))")
    Page<UserEntity> findBySearch(@Param("search") String search, Pageable pageable);

    @Query("SELECT u FROM UserEntity u JOIN u.roles r WHERE r.id = :roleId")
    Page<UserEntity> findByRoleId(@Param("roleId") Long roleId, Pageable pageable);

    Page<UserEntity> findByIsActive(Boolean isActive, Pageable pageable);
}
