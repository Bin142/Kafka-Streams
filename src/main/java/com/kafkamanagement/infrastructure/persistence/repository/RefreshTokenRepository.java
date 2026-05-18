package com.kafkamanagement.infrastructure.persistence.repository;

import com.kafkamanagement.domain.user.entity.RefreshTokenEntity;
import com.kafkamanagement.domain.user.entity.UserEntity;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.time.LocalDateTime;
import java.util.Optional;

@Repository
public interface RefreshTokenRepository extends JpaRepository<RefreshTokenEntity, Long> {

    Optional<RefreshTokenEntity> findByToken(String token);

    @Modifying
    @Query("UPDATE RefreshTokenEntity r SET r.revoked = true WHERE r.user = :user")
    void revokeAllByUser(@Param("user") UserEntity user);

    @Modifying
    @Query("DELETE FROM RefreshTokenEntity r WHERE r.expiresAt < :now OR r.revoked = true")
    void deleteExpiredAndRevoked(@Param("now") LocalDateTime now);

    @Query("SELECT COUNT(r) FROM RefreshTokenEntity r WHERE r.user = :user AND r.revoked = false AND r.expiresAt > :now")
    long countActiveTokensByUser(@Param("user") UserEntity user, @Param("now") LocalDateTime now);
}
