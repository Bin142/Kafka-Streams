package com.kafkamanagement.presentation.rest;

import com.kafkamanagement.application.consumergroup.ConsumerGroupService;
import com.kafkamanagement.application.consumergroup.dto.*;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.web.PageableDefault;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/v1/clusters/{clusterId}/consumer-groups")
@RequiredArgsConstructor
@Tag(name = "Consumer Groups", description = "Consumer group management APIs")
public class ConsumerGroupController {

    private final ConsumerGroupService consumerGroupService;

    @GetMapping
    @Operation(summary = "List consumer groups with filtering and pagination")
    public ResponseEntity<Page<ConsumerGroupDTO>> listConsumerGroups(
            @PathVariable String clusterId,
            @RequestParam(required = false) String search,
            @RequestParam(required = false) String state,
            @PageableDefault(size = 25) Pageable pageable) {
        return ResponseEntity.ok(consumerGroupService.listConsumerGroups(clusterId, pageable, search, state));
    }

    @GetMapping("/{groupId}")
    @Operation(summary = "Get consumer group details")
    public ResponseEntity<ConsumerGroupDetailDTO> getConsumerGroup(
            @PathVariable String clusterId,
            @PathVariable String groupId) {
        return ResponseEntity.ok(consumerGroupService.getConsumerGroup(clusterId, groupId));
    }

    @DeleteMapping("/{groupId}")
    @Operation(summary = "Delete consumer group")
    public ResponseEntity<Void> deleteConsumerGroup(
            @PathVariable String clusterId,
            @PathVariable String groupId) {
        consumerGroupService.deleteConsumerGroup(clusterId, groupId);
        return ResponseEntity.noContent().build();
    }

    @PostMapping("/{groupId}/offsets/reset")
    @Operation(summary = "Reset consumer group offsets")
    public ResponseEntity<Void> resetOffsets(
            @PathVariable String clusterId,
            @PathVariable String groupId,
            @Valid @RequestBody OffsetResetRequest request) {
        consumerGroupService.resetOffsets(clusterId, groupId, request);
        return ResponseEntity.ok().build();
    }
}
