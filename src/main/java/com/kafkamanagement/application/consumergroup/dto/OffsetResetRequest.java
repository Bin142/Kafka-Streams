package com.kafkamanagement.application.consumergroup.dto;

import jakarta.validation.constraints.NotNull;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;
import java.util.Map;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class OffsetResetRequest {

    @NotNull(message = "Reset type is required")
    private ResetType type;

    // For TO_OFFSET type
    private Map<String, Map<Integer, Long>> offsets; // topic -> partition -> offset

    // For TO_TIMESTAMP type
    private Long timestamp;

    // For SHIFT_BY type
    private Long shiftBy;

    // Optional: specific topics/partitions to reset
    private List<String> topics;
    private List<TopicPartitionRequest> partitions;

    public enum ResetType {
        TO_BEGINNING,
        TO_END,
        TO_OFFSET,
        TO_TIMESTAMP,
        SHIFT_BY
    }

    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    public static class TopicPartitionRequest {
        private String topic;
        private int partition;
    }
}
