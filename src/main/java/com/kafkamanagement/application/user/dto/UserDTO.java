package com.kafkamanagement.application.user.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.Instant;
import java.util.Set;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class UserDTO {
    private Long id;
    private String username;
    private String email;
    private String fullName;
    private boolean isAdmin;
    private boolean isActive;
    private Set<String> roles;
    private Instant createdAt;
    private Instant updatedAt;
}
