package com.kafkamanagement.common.exception;

import org.springframework.http.HttpStatus;

public class InvalidClusterException extends BusinessException {

    public InvalidClusterException(String clusterId) {
        super(
                String.format("Invalid cluster: %s", clusterId),
                HttpStatus.BAD_REQUEST,
                "INVALID_CLUSTER"
        );
    }
}
