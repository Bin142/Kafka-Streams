package com.kafkamanagement.application.consumergroup.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class ConsumerGroupDetailDTO {
    private String groupId;
    private String state;
    private String coordinator;
    private String partitionAssignor;
    private List<MemberDTO> members;
    private List<OffsetDTO> offsets;
    private long totalLag;

    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    public static class MemberDTO {
        private String memberId;
        private String clientId;
        private String host;
        private List<AssignmentDTO> assignments;
    }

    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    public static class AssignmentDTO {
        private String topic;
        private int partition;
    }

    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    public static class OffsetDTO {
        private String topic;
        private int partition;
        private long currentOffset;
        private long endOffset;
        private long lag;
        private String metadata;
    }
}
