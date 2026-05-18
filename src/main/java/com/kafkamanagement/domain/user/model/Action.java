package com.kafkamanagement.domain.user.model;

import lombok.Getter;

/**
 * Action types that can be performed on resources
 */
@Getter
public enum Action {
    READ("read"),
    CREATE("create"),
    UPDATE("update"),
    DELETE("delete"),
    MANAGE("manage");  // Full access - includes all actions

    private final String value;

    Action(String value) {
        this.value = value;
    }

    public static Action fromValue(String value) {
        for (Action action : values()) {
            if (action.value.equalsIgnoreCase(value) || action.name().equalsIgnoreCase(value)) {
                return action;
            }
        }
        throw new IllegalArgumentException("Unknown action: " + value);
    }

    /**
     * Check if this action includes another action
     * MANAGE includes all other actions
     */
    public boolean includes(Action other) {
        if (this == MANAGE) {
            return true;
        }
        return this == other;
    }
}
