package com.kafkamanagement.application.message.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class MessageBrowseResponse {
    private List<MessageDTO> messages;
    private String nextCursor;
    private String previousCursor;
    private boolean hasMore;
    private long totalPartitions;
    private long scannedMessages;
}
