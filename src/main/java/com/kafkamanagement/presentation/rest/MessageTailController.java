package com.kafkamanagement.presentation.rest;

import com.kafkamanagement.application.message.MessageTailService;
import com.kafkamanagement.application.message.dto.TailMessageDTO;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import lombok.RequiredArgsConstructor;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.*;
import reactor.core.publisher.Flux;

@RestController
@RequestMapping("/v1/clusters/{clusterId}/topics/{topicName}/messages")
@RequiredArgsConstructor
@Tag(name = "Message Tail", description = "Real-time message streaming APIs")
public class MessageTailController {

    private final MessageTailService messageTailService;

    @GetMapping(value = "/tail", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    @Operation(summary = "Real-time tail messages from topic (SSE)")
    public Flux<TailMessageDTO> tailMessages(
            @PathVariable String clusterId,
            @PathVariable String topicName,
            @RequestParam(required = false) Integer partition) {
        return messageTailService.tailMessages(clusterId, topicName, partition);
    }
}
