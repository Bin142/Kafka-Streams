package com.kafkamanagement.presentation.rest;

import com.kafkamanagement.application.message.MessageService;
import com.kafkamanagement.application.message.dto.*;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.time.Instant;

@RestController
@RequestMapping("/v1/clusters/{clusterId}/topics/{topicName}/messages")
@RequiredArgsConstructor
@Tag(name = "Messages", description = "Message browsing and production APIs")
public class MessageController {

    private final MessageService messageService;

    @GetMapping
    @Operation(summary = "Browse messages", description = "Browse messages from a topic with filtering and pagination")
    public ResponseEntity<MessageBrowseResponse> browseMessages(
            @PathVariable String clusterId,
            @PathVariable String topicName,
            @RequestParam(defaultValue = "NEWEST") MessageBrowseRequest.SortOrder sort,
            @RequestParam(required = false) Integer partition,
            @RequestParam(required = false) Instant afterTimestamp,
            @RequestParam(required = false) Instant beforeTimestamp,
            @RequestParam(required = false) String keyContains,
            @RequestParam(required = false) String valueContains,
            @RequestParam(required = false) String headerKey,
            @RequestParam(required = false) String headerValue,
            @RequestParam(defaultValue = "100") int limit,
            @RequestParam(required = false) String cursor) {

        MessageBrowseRequest request = MessageBrowseRequest.builder()
                .sort(sort)
                .partition(partition)
                .afterTimestamp(afterTimestamp)
                .beforeTimestamp(beforeTimestamp)
                .keyContains(keyContains)
                .valueContains(valueContains)
                .headerKey(headerKey)
                .headerValue(headerValue)
                .limit(Math.min(limit, 500))
                .cursor(cursor)
                .build();

        return ResponseEntity.ok(messageService.browseMessages(clusterId, topicName, request));
    }

    @PostMapping
    @Operation(summary = "Produce message", description = "Produce a message to a topic")
    public ResponseEntity<MessageDTO> produceMessage(
            @PathVariable String clusterId,
            @PathVariable String topicName,
            @Valid @RequestBody MessageProduceRequest request) {

        return ResponseEntity.ok(messageService.produceMessage(clusterId, topicName, request));
    }

    @DeleteMapping
    @Operation(summary = "Delete message (tombstone)", description = "Produce a tombstone message for compacted topics")
    public ResponseEntity<Void> deleteMessage(
            @PathVariable String clusterId,
            @PathVariable String topicName,
            @RequestParam String key,
            @RequestParam(required = false) Integer partition) {

        messageService.deleteMessage(clusterId, topicName, key, partition);
        return ResponseEntity.noContent().build();
    }

    @DeleteMapping("/all")
    @Operation(summary = "Empty topic", description = "Delete all records from a topic")
    public ResponseEntity<Void> emptyTopic(
            @PathVariable String clusterId,
            @PathVariable String topicName) {

        messageService.emptyTopic(clusterId, topicName);
        return ResponseEntity.noContent().build();
    }

    @PostMapping("/copy")
    @Operation(summary = "Copy messages", description = "Copy messages to another topic (same or different cluster)")
    public ResponseEntity<CopyMessageResponse> copyMessages(
            @PathVariable String clusterId,
            @PathVariable String topicName,
            @Valid @RequestBody CopyMessageRequest request) {

        return ResponseEntity.ok(messageService.copyMessages(clusterId, topicName, request));
    }

    @GetMapping("/export")
    @Operation(summary = "Export messages", description = "Export messages to JSON or CSV format")
    public ResponseEntity<String> exportMessages(
            @PathVariable String clusterId,
            @PathVariable String topicName,
            @RequestParam(defaultValue = "JSON") ExportMessageRequest.ExportFormat format,
            @RequestParam(required = false) Integer partition,
            @RequestParam(required = false) Instant afterTimestamp,
            @RequestParam(required = false) Instant beforeTimestamp,
            @RequestParam(required = false) String keyContains,
            @RequestParam(required = false) String valueContains,
            @RequestParam(defaultValue = "10000") int limit,
            @RequestParam(defaultValue = "true") boolean includeHeaders,
            @RequestParam(defaultValue = "true") boolean includeMetadata) {

        ExportMessageRequest request = ExportMessageRequest.builder()
                .format(format)
                .partition(partition)
                .afterTimestamp(afterTimestamp)
                .beforeTimestamp(beforeTimestamp)
                .keyContains(keyContains)
                .valueContains(valueContains)
                .limit(Math.min(limit, 100000))
                .includeHeaders(includeHeaders)
                .includeMetadata(includeMetadata)
                .build();

        String content = messageService.exportMessages(clusterId, topicName, request);
        
        String contentType = format == ExportMessageRequest.ExportFormat.CSV 
                ? "text/csv" 
                : MediaType.APPLICATION_JSON_VALUE;
        String filename = topicName + "_export." + format.name().toLowerCase();

        return ResponseEntity.ok()
                .header(HttpHeaders.CONTENT_DISPOSITION, "attachment; filename=\"" + filename + "\"")
                .contentType(MediaType.parseMediaType(contentType))
                .body(content);
    }
}
