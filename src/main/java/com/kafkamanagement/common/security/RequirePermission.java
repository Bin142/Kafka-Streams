package com.kafkamanagement.common.security;

import com.kafkamanagement.domain.user.model.Action;
import com.kafkamanagement.domain.user.model.Resource;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Annotation to require specific permission for a method
 */
@Target({ElementType.METHOD, ElementType.TYPE})
@Retention(RetentionPolicy.RUNTIME)
public @interface RequirePermission {
    Resource resource();
    Action action();
}
