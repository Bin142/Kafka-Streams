package com.kafkamanagement.application.auth;

import com.kafkamanagement.application.auth.dto.LoginRequest;
import com.kafkamanagement.application.auth.dto.LoginResponse;
import com.kafkamanagement.application.auth.dto.RefreshTokenRequest;
import com.kafkamanagement.common.exception.BusinessException;
import com.kafkamanagement.common.exception.UnauthorizedException;
import com.kafkamanagement.common.security.JwtTokenProvider;
import com.kafkamanagement.domain.user.entity.RefreshTokenEntity;
import com.kafkamanagement.domain.user.entity.UserEntity;
import com.kafkamanagement.infrastructure.persistence.repository.RefreshTokenRepository;
import com.kafkamanagement.infrastructure.persistence.repository.UserRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.security.authentication.AuthenticationManager;
import org.springframework.security.authentication.UsernamePasswordAuthenticationToken;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.AuthenticationException;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;

@Slf4j
@Service
@RequiredArgsConstructor
public class AuthService {

    private final AuthenticationManager authenticationManager;
    private final JwtTokenProvider jwtTokenProvider;
    private final UserRepository userRepository;
    private final RefreshTokenRepository refreshTokenRepository;

    @Transactional
    public LoginResponse login(LoginRequest request) {
        try {
            Authentication authentication = authenticationManager.authenticate(
                    new UsernamePasswordAuthenticationToken(request.getUsername(), request.getPassword())
            );

            UserEntity user = userRepository.findByUsername(request.getUsername())
                    .orElseThrow(() -> new UnauthorizedException("User not found"));

            if (!user.isActive()) {
                throw new BusinessException("User account is disabled");
            }

            // Update last login
            user.setLastLoginAt(Instant.now());
            userRepository.save(user);

            // Generate tokens
            String accessToken = jwtTokenProvider.generateAccessToken(
                    user.getId(), user.getUsername(), user.isAdmin()
            );
            String refreshToken = jwtTokenProvider.generateRefreshToken(user.getId(), user.getUsername());

            // Save refresh token
            saveRefreshToken(user, refreshToken);

            log.info("User {} logged in successfully", user.getUsername());

            return buildLoginResponse(user, accessToken, refreshToken);

        } catch (AuthenticationException e) {
            log.warn("Login failed for user {}: {}", request.getUsername(), e.getMessage());
            throw new UnauthorizedException("Invalid username or password");
        }
    }

    @Transactional
    public LoginResponse refreshToken(RefreshTokenRequest request) {
        String refreshToken = request.getRefreshToken();

        // Validate refresh token
        if (!jwtTokenProvider.validateToken(refreshToken)) {
            throw new UnauthorizedException("Invalid refresh token");
        }

        if (!jwtTokenProvider.isRefreshToken(refreshToken)) {
            throw new UnauthorizedException("Invalid token type");
        }

        // Check if token exists and is valid in database
        RefreshTokenEntity tokenEntity = refreshTokenRepository.findByToken(refreshToken)
                .orElseThrow(() -> new UnauthorizedException("Refresh token not found"));

        if (!tokenEntity.isValid()) {
            throw new UnauthorizedException("Refresh token is expired or revoked");
        }

        UserEntity user = tokenEntity.getUser();

        if (!user.isActive()) {
            throw new BusinessException("User account is disabled");
        }

        // Revoke old refresh token
        tokenEntity.setRevoked(true);
        refreshTokenRepository.save(tokenEntity);

        // Generate new tokens
        String newAccessToken = jwtTokenProvider.generateAccessToken(
                user.getId(), user.getUsername(), user.isAdmin()
        );
        String newRefreshToken = jwtTokenProvider.generateRefreshToken(user.getId(), user.getUsername());

        // Save new refresh token
        saveRefreshToken(user, newRefreshToken);

        log.info("Token refreshed for user {}", user.getUsername());

        return buildLoginResponse(user, newAccessToken, newRefreshToken);
    }

    @Transactional
    public void logout(String refreshToken) {
        if (refreshToken != null) {
            refreshTokenRepository.findByToken(refreshToken)
                    .ifPresent(token -> {
                        token.setRevoked(true);
                        refreshTokenRepository.save(token);
                        log.info("User {} logged out", token.getUser().getUsername());
                    });
        }
    }

    @Transactional
    public void logoutAll(Long userId) {
        UserEntity user = userRepository.findById(userId)
                .orElseThrow(() -> new UnauthorizedException("User not found"));
        refreshTokenRepository.revokeAllByUser(user);
        log.info("All sessions revoked for user {}", user.getUsername());
    }

    private void saveRefreshToken(UserEntity user, String token) {
        LocalDateTime expiresAt = LocalDateTime.now()
                .plusSeconds(jwtTokenProvider.getRefreshTokenExpiration() / 1000);

        RefreshTokenEntity refreshTokenEntity = RefreshTokenEntity.builder()
                .user(user)
                .token(token)
                .expiresAt(expiresAt)
                .build();

        refreshTokenRepository.save(refreshTokenEntity);
    }

    private LoginResponse buildLoginResponse(UserEntity user, String accessToken, String refreshToken) {
        List<String> roles = user.getRoles().stream()
                .map(role -> role.getName())
                .toList();

        List<LoginResponse.PermissionInfo> permissions = user.getAllPermissions().stream()
                .map(p -> LoginResponse.PermissionInfo.builder()
                        .resource(p.getResource().getValue())
                        .action(p.getAction().getValue())
                        .resourcePattern(p.getResourcePattern())
                        .clusterIds(new ArrayList<>(p.getClusterIds()))
                        .build())
                .toList();

        LoginResponse.UserInfo userInfo = LoginResponse.UserInfo.builder()
                .id(user.getId())
                .username(user.getUsername())
                .email(user.getEmail())
                .fullName(user.getFullName())
                .isAdmin(user.isAdmin())
                .roles(roles)
                .permissions(permissions)
                .build();

        return LoginResponse.builder()
                .accessToken(accessToken)
                .refreshToken(refreshToken)
                .tokenType("Bearer")
                .expiresIn(jwtTokenProvider.getAccessTokenExpiration() / 1000)
                .user(userInfo)
                .build();
    }
}
