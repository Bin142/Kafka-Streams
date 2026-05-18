package com.kafkamanagement.application.message;

import com.kafkamanagement.application.message.dto.*;
import com.kafkamanagement.common.exception.BusinessException;
import com.kafkamanagement.common.security.PermissionChecker;
import com.kafkamanagement.domain.user.model.Action;
import com.kafkamanagement.domain.user.model.Resource;
import com.kafkamanagement.infrastructure.kafka.KafkaAdminWrapper;
import com.kafkamanagement.infrastructure.kafka.KafkaClientFactory;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Header;
import org.springframework.stereotype.Service;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

@Slf4j
@Service
@RequiredArgsConstructor
public class MessageService {

    private final KafkaClientFactory kafkaClientFactory;
    private final KafkaAdminWrapper kafkaAdminWrapper;
    private final PermissionChecker permissionChecker;

    private static final Duration POLL_TIMEOUT = Duration.ofSeconds(5);
    private static final int MAX_POLL_RECORDS = 500;

    /**
     * Browse messages from a topic
     */
    public MessageBrowseResponse browseMessages(String clusterId, String topicName, MessageBrowseRequest request) {
        permissionChecker.checkPermission(clusterId, Resource.TOPIC_DATA, Action.READ, topicName);

        try (KafkaConsumer<String, String> consumer = kafkaClientFactory.createConsumer(clusterId)) {
            // Get partitions
            List<TopicPartition> partitions = getTargetPartitions(consumer, topicName, request.getPartition());
            consumer.assign(partitions);

            // Seek to appropriate position
            seekToPosition(consumer, clusterId, partitions, request);

            // Poll messages
            List<MessageDTO> messages = new ArrayList<>();
            long scannedCount = 0;
            int limit = Math.min(request.getLimit(), MAX_POLL_RECORDS);

            long startTime = System.currentTimeMillis();
            long maxPollTime = 10000; // 10 seconds max

            while (messages.size() < limit && (System.currentTimeMillis() - startTime) < maxPollTime) {
                ConsumerRecords<String, String> records = consumer.poll(POLL_TIMEOUT);
                
                if (records.isEmpty()) {
                    break;
                }

                for (ConsumerRecord<String, String> record : records) {
                    scannedCount++;
                    
                    if (matchesFilters(record, request)) {
                        messages.add(convertToDTO(record));
                        
                        if (messages.size() >= limit) {
                            break;
                        }
                    }
                }
            }

            // Sort by timestamp/offset
            if (request.getSort() == MessageBrowseRequest.SortOrder.NEWEST) {
                messages.sort((a, b) -> {
                    int cmp = b.getTimestamp().compareTo(a.getTimestamp());
                    return cmp != 0 ? cmp : Long.compare(b.getOffset(), a.getOffset());
                });
            } else {
                messages.sort((a, b) -> {
                    int cmp = a.getTimestamp().compareTo(b.getTimestamp());
                    return cmp != 0 ? cmp : Long.compare(a.getOffset(), b.getOffset());
                });
            }

            // Build cursor for pagination
            String nextCursor = null;
            if (!messages.isEmpty() && messages.size() >= limit) {
                MessageDTO lastMsg = messages.get(messages.size() - 1);
                nextCursor = lastMsg.getPartition() + ":" + (lastMsg.getOffset() + 1);
            }

            return MessageBrowseResponse.builder()
                    .messages(messages)
                    .nextCursor(nextCursor)
                    .hasMore(messages.size() >= limit)
                    .totalPartitions(partitions.size())
                    .scannedMessages(scannedCount)
                    .build();

        } catch (Exception e) {
            log.error("Failed to browse messages from topic {} in cluster {}", topicName, clusterId, e);
            throw new BusinessException("Failed to browse messages: " + e.getMessage());
        }
    }

    /**
     * Produce a message to a topic
     */
    public MessageDTO produceMessage(String clusterId, String topicName, MessageProduceRequest request) {
        permissionChecker.checkPermission(clusterId, Resource.TOPIC_DATA, Action.CREATE, topicName);

        try (KafkaProducer<String, String> producer = kafkaClientFactory.createProducer(clusterId)) {
            ProducerRecord<String, String> record = new ProducerRecord<>(
                    topicName,
                    request.getPartition(),
                    request.getTimestamp(),
                    request.getKey(),
                    request.getValue()
            );

            // Add headers
            if (request.getHeaders() != null) {
                request.getHeaders().forEach((key, value) -> 
                    record.headers().add(key, value.getBytes(StandardCharsets.UTF_8))
                );
            }

            Future<RecordMetadata> future = producer.send(record);
            RecordMetadata metadata = future.get();

            log.info("Produced message to topic {} partition {} offset {} in cluster {}", 
                    topicName, metadata.partition(), metadata.offset(), clusterId);

            return MessageDTO.builder()
                    .partition(metadata.partition())
                    .offset(metadata.offset())
                    .timestamp(Instant.ofEpochMilli(metadata.timestamp()))
                    .key(request.getKey())
                    .value(request.getValue())
                    .headers(request.getHeaders())
                    .build();

        } catch (ExecutionException | InterruptedException e) {
            log.error("Failed to produce message to topic {} in cluster {}", topicName, clusterId, e);
            throw new BusinessException("Failed to produce message: " + e.getMessage());
        }
    }

    /**
     * Delete message (produce tombstone for compacted topics)
     */
    public void deleteMessage(String clusterId, String topicName, String key, Integer partition) {
        permissionChecker.checkPermission(clusterId, Resource.TOPIC_DATA, Action.DELETE, topicName);

        if (key == null || key.isBlank()) {
            throw new BusinessException("Key is required for tombstone message");
        }

        try (KafkaProducer<String, String> producer = kafkaClientFactory.createProducer(clusterId)) {
            ProducerRecord<String, String> tombstone = new ProducerRecord<>(
                    topicName,
                    partition,
                    key,
                    null  // null value = tombstone
            );

            Future<RecordMetadata> future = producer.send(tombstone);
            RecordMetadata metadata = future.get();

            log.info("Produced tombstone for key {} to topic {} partition {} in cluster {}", 
                    key, topicName, metadata.partition(), clusterId);

        } catch (ExecutionException | InterruptedException e) {
            log.error("Failed to delete message from topic {} in cluster {}", topicName, clusterId, e);
            throw new BusinessException("Failed to delete message: " + e.getMessage());
        }
    }

    /**
     * Empty topic (delete all records)
     */
    public void emptyTopic(String clusterId, String topicName) {
        permissionChecker.checkPermission(clusterId, Resource.TOPIC_DATA, Action.DELETE, topicName);

        try {
            kafkaAdminWrapper.deleteAllRecords(clusterId, topicName);
            log.info("Emptied topic {} in cluster {}", topicName, clusterId);
        } catch (ExecutionException | InterruptedException e) {
            log.error("Failed to empty topic {} in cluster {}", topicName, clusterId, e);
            throw new BusinessException("Failed to empty topic: " + e.getMessage());
        }
    }

    /**
     * Copy messages between topics (same or different clusters)
     */
    public CopyMessageResponse copyMessages(String sourceClusterId, String sourceTopic, CopyMessageRequest request) {
        permissionChecker.checkPermission(sourceClusterId, Resource.TOPIC_DATA, Action.READ, sourceTopic);
        permissionChecker.checkPermission(request.getDestinationClusterId(), Resource.TOPIC_DATA, Action.CREATE, request.getDestinationTopic());

        long startTime = System.currentTimeMillis();
        long copiedCount = 0;
        long failedCount = 0;

        try (KafkaConsumer<String, String> consumer = kafkaClientFactory.createConsumer(sourceClusterId);
             KafkaProducer<String, String> producer = kafkaClientFactory.createProducer(request.getDestinationClusterId())) {

            // Assign all partitions
            List<TopicPartition> partitions = consumer.partitionsFor(sourceTopic).stream()
                    .map(info -> new TopicPartition(sourceTopic, info.partition()))
                    .collect(Collectors.toList());
            consumer.assign(partitions);

            // Seek based on timestamp filter
            if (request.getAfterTimestamp() != null) {
                Map<TopicPartition, Long> timestampMap = partitions.stream()
                        .collect(Collectors.toMap(tp -> tp, tp -> request.getAfterTimestamp().toEpochMilli()));
                Map<TopicPartition, org.apache.kafka.clients.consumer.OffsetAndTimestamp> offsets = 
                        consumer.offsetsForTimes(timestampMap);
                offsets.forEach((tp, offsetAndTimestamp) -> {
                    if (offsetAndTimestamp != null) {
                        consumer.seek(tp, offsetAndTimestamp.offset());
                    }
                });
            } else {
                consumer.seekToBeginning(partitions);
            }

            int limit = request.getLimit();
            long maxPollTime = 60000; // 60 seconds max
            long pollStartTime = System.currentTimeMillis();

            while (copiedCount < limit && (System.currentTimeMillis() - pollStartTime) < maxPollTime) {
                ConsumerRecords<String, String> records = consumer.poll(POLL_TIMEOUT);
                
                if (records.isEmpty()) {
                    break;
                }

                for (ConsumerRecord<String, String> record : records) {
                    // Apply filters
                    if (request.getKeyContains() != null && 
                        (record.key() == null || !record.key().contains(request.getKeyContains()))) {
                        continue;
                    }
                    if (request.getBeforeTimestamp() != null && 
                        record.timestamp() > request.getBeforeTimestamp().toEpochMilli()) {
                        continue;
                    }

                    try {
                        ProducerRecord<String, String> destRecord = new ProducerRecord<>(
                                request.getDestinationTopic(),
                                request.isPreservePartition() ? record.partition() : null,
                                request.isPreserveTimestamp() ? record.timestamp() : null,
                                record.key(),
                                record.value()
                        );

                        // Copy headers
                        for (Header header : record.headers()) {
                            destRecord.headers().add(header);
                        }

                        producer.send(destRecord).get();
                        copiedCount++;

                        if (copiedCount >= limit) {
                            break;
                        }
                    } catch (Exception e) {
                        log.warn("Failed to copy message at offset {}", record.offset(), e);
                        failedCount++;
                    }
                }
            }

            producer.flush();

            long durationMs = System.currentTimeMillis() - startTime;
            log.info("Copied {} messages from {}:{} to {}:{} in {}ms", 
                    copiedCount, sourceClusterId, sourceTopic, 
                    request.getDestinationClusterId(), request.getDestinationTopic(), durationMs);

            return CopyMessageResponse.builder()
                    .copiedCount(copiedCount)
                    .failedCount(failedCount)
                    .sourceClusterId(sourceClusterId)
                    .sourceTopic(sourceTopic)
                    .destinationClusterId(request.getDestinationClusterId())
                    .destinationTopic(request.getDestinationTopic())
                    .durationMs(durationMs)
                    .build();

        } catch (Exception e) {
            log.error("Failed to copy messages from {}:{} to {}:{}", 
                    sourceClusterId, sourceTopic, 
                    request.getDestinationClusterId(), request.getDestinationTopic(), e);
            throw new BusinessException("Failed to copy messages: " + e.getMessage());
        }
    }

    /**
     * Export messages to JSON or CSV format
     */
    public String exportMessages(String clusterId, String topicName, ExportMessageRequest request) {
        permissionChecker.checkPermission(clusterId, Resource.TOPIC_DATA, Action.READ, topicName);

        MessageBrowseRequest browseRequest = MessageBrowseRequest.builder()
                .partition(request.getPartition())
                .afterTimestamp(request.getAfterTimestamp())
                .beforeTimestamp(request.getBeforeTimestamp())
                .keyContains(request.getKeyContains())
                .valueContains(request.getValueContains())
                .limit(request.getLimit())
                .sort(MessageBrowseRequest.SortOrder.OLDEST)
                .build();

        MessageBrowseResponse response = browseMessages(clusterId, topicName, browseRequest);

        if (request.getFormat() == ExportMessageRequest.ExportFormat.CSV) {
            return exportToCsv(response.getMessages(), request);
        } else {
            return exportToJson(response.getMessages(), request);
        }
    }

    // ==================== Helper Methods ====================

    private List<TopicPartition> getTargetPartitions(KafkaConsumer<String, String> consumer, 
                                                      String topicName, Integer partition) {
        if (partition != null) {
            return List.of(new TopicPartition(topicName, partition));
        }
        return consumer.partitionsFor(topicName).stream()
                .map(info -> new TopicPartition(topicName, info.partition()))
                .collect(Collectors.toList());
    }

    private void seekToPosition(KafkaConsumer<String, String> consumer, String clusterId,
                                List<TopicPartition> partitions, MessageBrowseRequest request) 
            throws ExecutionException, InterruptedException {
        
        // Handle cursor-based pagination
        if (request.getCursor() != null && !request.getCursor().isBlank()) {
            String[] parts = request.getCursor().split(":");
            if (parts.length == 2) {
                int cursorPartition = Integer.parseInt(parts[0]);
                long cursorOffset = Long.parseLong(parts[1]);
                for (TopicPartition tp : partitions) {
                    if (tp.partition() == cursorPartition) {
                        consumer.seek(tp, cursorOffset);
                    } else {
                        consumer.seekToEnd(List.of(tp));
                    }
                }
                return;
            }
        }

        // Handle timestamp-based seek
        if (request.getAfterTimestamp() != null) {
            Map<TopicPartition, Long> timestampMap = partitions.stream()
                    .collect(Collectors.toMap(tp -> tp, tp -> request.getAfterTimestamp().toEpochMilli()));
            Map<TopicPartition, org.apache.kafka.clients.consumer.OffsetAndTimestamp> offsets = 
                    consumer.offsetsForTimes(timestampMap);
            for (TopicPartition tp : partitions) {
                org.apache.kafka.clients.consumer.OffsetAndTimestamp offsetAndTimestamp = offsets.get(tp);
                if (offsetAndTimestamp != null) {
                    consumer.seek(tp, offsetAndTimestamp.offset());
                } else {
                    consumer.seekToEnd(List.of(tp));
                }
            }
            return;
        }

        // Default: seek based on sort order
        if (request.getSort() == MessageBrowseRequest.SortOrder.NEWEST) {
            // For newest, we need to seek to end and go backwards
            Map<TopicPartition, Long> endOffsets = kafkaAdminWrapper.getEndOffsets(clusterId, partitions);
            Map<TopicPartition, Long> beginOffsets = kafkaAdminWrapper.getBeginningOffsets(clusterId, partitions);
            
            for (TopicPartition tp : partitions) {
                long endOffset = endOffsets.getOrDefault(tp, 0L);
                long beginOffset = beginOffsets.getOrDefault(tp, 0L);
                // Seek to position that allows reading last N messages
                long seekOffset = Math.max(beginOffset, endOffset - request.getLimit());
                consumer.seek(tp, seekOffset);
            }
        } else {
            consumer.seekToBeginning(partitions);
        }
    }

    private boolean matchesFilters(ConsumerRecord<String, String> record, MessageBrowseRequest request) {
        // Timestamp filter
        if (request.getAfterTimestamp() != null && 
            record.timestamp() < request.getAfterTimestamp().toEpochMilli()) {
            return false;
        }
        if (request.getBeforeTimestamp() != null && 
            record.timestamp() > request.getBeforeTimestamp().toEpochMilli()) {
            return false;
        }

        // Key filter
        if (request.getKeyContains() != null && !request.getKeyContains().isBlank()) {
            if (record.key() == null || !record.key().contains(request.getKeyContains())) {
                return false;
            }
        }

        // Value filter
        if (request.getValueContains() != null && !request.getValueContains().isBlank()) {
            if (record.value() == null || !record.value().contains(request.getValueContains())) {
                return false;
            }
        }

        // Header filter
        if (request.getHeaderKey() != null && !request.getHeaderKey().isBlank()) {
            boolean headerMatch = StreamSupport.stream(record.headers().spliterator(), false)
                    .anyMatch(header -> {
                        if (!header.key().equals(request.getHeaderKey())) {
                            return false;
                        }
                        if (request.getHeaderValue() != null) {
                            String headerValue = new String(header.value(), StandardCharsets.UTF_8);
                            return headerValue.contains(request.getHeaderValue());
                        }
                        return true;
                    });
            if (!headerMatch) {
                return false;
            }
        }

        return true;
    }

    private MessageDTO convertToDTO(ConsumerRecord<String, String> record) {
        Map<String, String> headers = new HashMap<>();
        for (Header header : record.headers()) {
            headers.put(header.key(), new String(header.value(), StandardCharsets.UTF_8));
        }

        return MessageDTO.builder()
                .partition(record.partition())
                .offset(record.offset())
                .timestamp(Instant.ofEpochMilli(record.timestamp()))
                .timestampType(record.timestampType().name())
                .key(record.key())
                .value(record.value())
                .headers(headers)
                .keyFormat(detectFormat(record.key()))
                .valueFormat(detectFormat(record.value()))
                .build();
    }

    private String detectFormat(String content) {
        if (content == null) {
            return "NULL";
        }
        String trimmed = content.trim();
        if ((trimmed.startsWith("{") && trimmed.endsWith("}")) || 
            (trimmed.startsWith("[") && trimmed.endsWith("]"))) {
            return "JSON";
        }
        return "STRING";
    }

    private String exportToJson(List<MessageDTO> messages, ExportMessageRequest request) {
        StringBuilder sb = new StringBuilder();
        sb.append("[\n");
        
        for (int i = 0; i < messages.size(); i++) {
            MessageDTO msg = messages.get(i);
            sb.append("  {\n");
            
            if (request.isIncludeMetadata()) {
                sb.append("    \"partition\": ").append(msg.getPartition()).append(",\n");
                sb.append("    \"offset\": ").append(msg.getOffset()).append(",\n");
                sb.append("    \"timestamp\": \"").append(msg.getTimestamp()).append("\",\n");
            }
            
            sb.append("    \"key\": ").append(escapeJson(msg.getKey())).append(",\n");
            sb.append("    \"value\": ").append(escapeJson(msg.getValue()));
            
            if (request.isIncludeHeaders() && msg.getHeaders() != null && !msg.getHeaders().isEmpty()) {
                sb.append(",\n    \"headers\": {");
                List<String> headerEntries = msg.getHeaders().entrySet().stream()
                        .map(e -> "\"" + e.getKey() + "\": " + escapeJson(e.getValue()))
                        .collect(Collectors.toList());
                sb.append(String.join(", ", headerEntries));
                sb.append("}");
            }
            
            sb.append("\n  }");
            if (i < messages.size() - 1) {
                sb.append(",");
            }
            sb.append("\n");
        }
        
        sb.append("]");
        return sb.toString();
    }

    private String exportToCsv(List<MessageDTO> messages, ExportMessageRequest request) {
        StringBuilder sb = new StringBuilder();
        
        // Header row
        List<String> headers = new ArrayList<>();
        if (request.isIncludeMetadata()) {
            headers.addAll(List.of("partition", "offset", "timestamp"));
        }
        headers.addAll(List.of("key", "value"));
        if (request.isIncludeHeaders()) {
            headers.add("headers");
        }
        sb.append(String.join(",", headers)).append("\n");
        
        // Data rows
        for (MessageDTO msg : messages) {
            List<String> values = new ArrayList<>();
            if (request.isIncludeMetadata()) {
                values.add(String.valueOf(msg.getPartition()));
                values.add(String.valueOf(msg.getOffset()));
                values.add(msg.getTimestamp().toString());
            }
            values.add(escapeCsv(msg.getKey()));
            values.add(escapeCsv(msg.getValue()));
            if (request.isIncludeHeaders()) {
                values.add(escapeCsv(msg.getHeaders() != null ? msg.getHeaders().toString() : ""));
            }
            sb.append(String.join(",", values)).append("\n");
        }
        
        return sb.toString();
    }

    private String escapeJson(String value) {
        if (value == null) {
            return "null";
        }
        return "\"" + value
                .replace("\\", "\\\\")
                .replace("\"", "\\\"")
                .replace("\n", "\\n")
                .replace("\r", "\\r")
                .replace("\t", "\\t") + "\"";
    }

    private String escapeCsv(String value) {
        if (value == null) {
            return "";
        }
        if (value.contains(",") || value.contains("\"") || value.contains("\n")) {
            return "\"" + value.replace("\"", "\"\"") + "\"";
        }
        return value;
    }
}
