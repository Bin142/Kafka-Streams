package com.kafkamanagement.presentation.rest;

import com.kafkamanagement.application.topic.TopicService;
import com.kafkamanagement.application.topic.dto.*;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.web.PageableDefault;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/v1/clusters/{clusterId}/topics")
@RequiredArgsConstructor
@Tag(name = "Topics", description = "Topic management APIs")
public class TopicController {

    private final TopicService topicService;

    @GetMapping
    @Operation(summary = "List topics with filtering and pagination")
    public ResponseEntity<Page<TopicDTO>> listTopics(
            @PathVariable String clusterId,
            @RequestParam(required = false) String search,
            @RequestParam(defaultValue = "HIDE_INTERNAL") TopicListView view,
            @PageableDefault(size = 25) Pageable pageable) {
        return ResponseEntity.ok(topicService.listTopics(clusterId, pageable, search, view));
    }

    @GetMapping("/{topicName}")
    @Operation(summary = "Get topic details")
    public ResponseEntity<TopicDetailDTO> getTopic(
            @PathVariable String clusterId,
            @PathVariable String topicName) {
        return ResponseEntity.ok(topicService.getTopic(clusterId, topicName));
    }

    @PostMapping
    @Operation(summary = "Create new topic")
    public ResponseEntity<TopicDTO> createTopic(
            @PathVariable String clusterId,
            @Valid @RequestBody TopicCreateRequest request) {
        return ResponseEntity.status(HttpStatus.CREATED)
                .body(topicService.createTopic(clusterId, request));
    }

    @DeleteMapping("/{topicName}")
    @Operation(summary = "Delete topic")
    public ResponseEntity<Void> deleteTopic(
            @PathVariable String clusterId,
            @PathVariable String topicName) {
        topicService.deleteTopic(clusterId, topicName);
        return ResponseEntity.noContent().build();
    }

    @PostMapping("/{topicName}/partitions")
    @Operation(summary = "Increase topic partitions")
    public ResponseEntity<Void> increasePartitions(
            @PathVariable String clusterId,
            @PathVariable String topicName,
            @RequestParam int partitions) {
        topicService.increasePartitions(clusterId, topicName, partitions);
        return ResponseEntity.accepted().build();
    }

    @GetMapping("/{topicName}/configs")
    @Operation(summary = "Get topic configurations")
    public ResponseEntity<List<TopicConfigDTO>> getTopicConfigs(
            @PathVariable String clusterId,
            @PathVariable String topicName) {
        return ResponseEntity.ok(topicService.getTopicConfigs(clusterId, topicName));
    }

    @PutMapping("/{topicName}/configs")
    @Operation(summary = "Update topic configurations")
    public ResponseEntity<Void> updateTopicConfigs(
            @PathVariable String clusterId,
            @PathVariable String topicName,
            @RequestBody Map<String, String> configs) {
        topicService.updateTopicConfigs(clusterId, topicName, configs);
        return ResponseEntity.ok().build();
    }
}
