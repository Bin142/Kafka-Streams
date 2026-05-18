package com.kafkamanagement.application.topic.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class TopicConfigDTO {
    private String name;
    private String value;
    private String defaultValue;
    private boolean isDefault;
    private boolean isReadOnly;
    private boolean isSensitive;
    private String source;
    private String documentation;
}
