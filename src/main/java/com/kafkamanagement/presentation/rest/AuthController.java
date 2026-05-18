package com.kafkamanagement.presentation.rest;

import com.kafkamanagement.application.auth.AuthService;
import com.kafkamanagement.application.auth.dto.LoginRequest;
import com.kafkamanagement.application.auth.dto.LoginResponse;
import com.kafkamanagement.application.auth.dto.RefreshTokenRequest;
import com.kafkamanagement.common.security.CurrentUser;
import com.kafkamanagement.common.security.UserPrincipal;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/v1/auth")
@RequiredArgsConstructor
@Tag(name = "Authentication", description = "Authentication APIs")
public class AuthController {

    private final AuthService authService;

    @PostMapping("/login")
    @Operation(summary = "Login with username and password")
    public ResponseEntity<LoginResponse> login(@Valid @RequestBody LoginRequest request) {
        return ResponseEntity.ok(authService.login(request));
    }

    @PostMapping("/refresh")
    @Operation(summary = "Refresh access token using refresh token")
    public ResponseEntity<LoginResponse> refreshToken(@Valid @RequestBody RefreshTokenRequest request) {
        return ResponseEntity.ok(authService.refreshToken(request));
    }

    @PostMapping("/logout")
    @Operation(summary = "Logout and invalidate refresh token")
    public ResponseEntity<Void> logout(@RequestBody(required = false) RefreshTokenRequest request) {
        String refreshToken = request != null ? request.getRefreshToken() : null;
        authService.logout(refreshToken);
        return ResponseEntity.ok().build();
    }

    @PostMapping("/logout-all")
    @Operation(summary = "Logout from all devices")
    public ResponseEntity<Void> logoutAll(@CurrentUser UserPrincipal currentUser) {
        authService.logoutAll(currentUser.getId());
        return ResponseEntity.ok().build();
    }

    @GetMapping("/me")
    @Operation(summary = "Get current user info")
    public ResponseEntity<LoginResponse.UserInfo> getCurrentUser(@CurrentUser UserPrincipal currentUser) {
        LoginResponse.UserInfo userInfo = LoginResponse.UserInfo.builder()
                .id(currentUser.getId())
                .username(currentUser.getUsername())
                .email(currentUser.getEmail())
                .isAdmin(currentUser.isAdmin())
                .build();
        return ResponseEntity.ok(userInfo);
    }
}
