package com.kafkamanagement.application.message;

import com.kafkamanagement.application.message.dto.TailMessageDTO;
import com.kafkamanagement.common.security.PermissionChecker;
import com.kafkamanagement.domain.user.model.Action;
import com.kafkamanagement.domain.user.model.Resource;
import com.kafkamanagement.infrastructure.kafka.KafkaClientFactory;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Header;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

@Slf4j
@Service
@RequiredArgsConstructor
public class MessageTailService {

    private final KafkaClientFactory kafkaClientFactory;
    private final PermissionChecker permissionChecker;
    
    private final Map<String, AtomicBoolean> activeStreams = new ConcurrentHashMap<>();
    private final ExecutorService executorService = Executors.newCachedThreadPool();

    /**
     * Create a real-time message stream (SSE) for a topic
     */
    public Flux<TailMessageDTO> tailMessages(String clusterId, String topicName, Integer partition) {
        permissionChecker.checkPermission(clusterId, Resource.TOPIC_DATA, Action.READ, topicName);
        
        String streamId = UUID.randomUUID().toString();
        AtomicBoolean running = new AtomicBoolean(true);
        activeStreams.put(streamId, running);
        
        return Flux.create(sink -> {
            executorService.submit(() -> {
                KafkaConsumer<byte[], byte[]> consumer = null;
                try {
                    consumer = kafkaClientFactory.createConsumer(clusterId);
                    
                    // Get partitions to subscribe
                    List<TopicPartition> partitions;
                    if (partition != null) {
                        partitions = Collections.singletonList(new TopicPartition(topicName, partition));
                    } else {
                        partitions = consumer.partitionsFor(topicName).stream()
                                .map(info -> new TopicPartition(topicName, info.partition()))
                                .toList();
                    }
                    
                    // Assign partitions and seek to end
                    consumer.assign(partitions);
                    consumer.seekToEnd(partitions);
                    
                    log.info("Started tailing topic {} in cluster {} (streamId: {})", topicName, clusterId, streamId);
                    
                    while (running.get() && !sink.isCancelled()) {
                        ConsumerRecords<byte[], byte[]> records = consumer.poll(Duration.ofMillis(500));
                        
                        for (ConsumerRecord<byte[], byte[]> record : records) {
                            if (!running.get() || sink.isCancelled()) break;
                            
                            TailMessageDTO message = convertToDTO(record);
                            sink.next(message);
                        }
                    }
                    
                } catch (Exception e) {
                    if (running.get()) {
                        log.error("Error while tailing topic {} in cluster {}", topicName, clusterId, e);
                        sink.error(e);
                    }
                } finally {
                    if (consumer != null) {
                        try {
                            consumer.close();
                        } catch (Exception e) {
                            log.warn("Error closing consumer", e);
                        }
                    }
                    activeStreams.remove(streamId);
                    log.info("Stopped tailing topic {} in cluster {} (streamId: {})", topicName, clusterId, streamId);
                    sink.complete();
                }
            });
            
            // Handle cancellation
            sink.onCancel(() -> {
                running.set(false);
                activeStreams.remove(streamId);
            });
            
            sink.onDispose(() -> {
                running.set(false);
                activeStreams.remove(streamId);
            });
            
        }, FluxSink.OverflowStrategy.LATEST);
    }

    /**
     * Stop all active streams (for cleanup)
     */
    public void stopAllStreams() {
        activeStreams.values().forEach(running -> running.set(false));
        activeStreams.clear();
    }

    private TailMessageDTO convertToDTO(ConsumerRecord<byte[], byte[]> record) {
        String key = record.key() != null ? new String(record.key(), StandardCharsets.UTF_8) : null;
        String value = record.value() != null ? new String(record.value(), StandardCharsets.UTF_8) : null;
        
        Map<String, String> headers = new HashMap<>();
        for (Header header : record.headers()) {
            headers.put(header.key(), 
                    header.value() != null ? new String(header.value(), StandardCharsets.UTF_8) : null);
        }
        
        return TailMessageDTO.builder()
                .partition(record.partition())
                .offset(record.offset())
                .timestamp(record.timestamp())
                .timestampType(record.timestampType().name())
                .key(key)
                .value(value)
                .headers(headers)
                .keyFormat(detectFormat(key))
                .valueFormat(detectFormat(value))
                .build();
    }

    private String detectFormat(String content) {
        if (content == null) return "NULL";
        content = content.trim();
        if (content.isEmpty()) return "EMPTY";
        if ((content.startsWith("{") && content.endsWith("}")) || 
            (content.startsWith("[") && content.endsWith("]"))) {
            return "JSON";
        }
        if (content.startsWith("<") && content.endsWith(">")) {
            return "XML";
        }
        return "STRING";
    }
}
