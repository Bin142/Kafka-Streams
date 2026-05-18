package com.kafkamanagement.domain.user.model;

import lombok.Getter;

/**
 * Resource types that can be protected by permissions
 */
@Getter
public enum Resource {
    CLUSTER("cluster"),
    TOPIC("topic"),
    TOPIC_DATA("topic_data"),
    CONSUMER_GROUP("consumer_group"),
    SCHEMA("schema"),
    CONNECT("connect"),
    ACL("acl"),
    USER("user"),
    ROLE("role");

    private final String value;

    Resource(String value) {
        this.value = value;
    }

    public static Resource fromValue(String value) {
        for (Resource resource : values()) {
            if (resource.value.equalsIgnoreCase(value) || resource.name().equalsIgnoreCase(value)) {
                return resource;
            }
        }
        throw new IllegalArgumentException("Unknown resource: " + value);
    }
}
