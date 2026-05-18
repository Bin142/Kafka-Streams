package com.kafkamanagement.common.security;

import com.kafkamanagement.domain.user.model.Action;
import com.kafkamanagement.domain.user.model.Resource;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.reflect.MethodSignature;
import org.springframework.stereotype.Component;

@Slf4j
@Aspect
@Component
@RequiredArgsConstructor
public class PermissionAspect {

    private final PermissionChecker permissionChecker;

    @Around("@annotation(requirePermission)")
    public Object checkPermission(ProceedingJoinPoint joinPoint, RequirePermission requirePermission) throws Throwable {
        Resource resource = requirePermission.resource();
        Action action = requirePermission.action();

        String clusterId = extractClusterId(joinPoint);
        String resourceName = extractResourceName(joinPoint);

        log.debug("Checking permission: resource={}, action={}, clusterId={}, resourceName={}",
                resource, action, clusterId, resourceName);

        permissionChecker.checkPermission(clusterId, resource, action, resourceName);

        return joinPoint.proceed();
    }

    private String extractClusterId(ProceedingJoinPoint joinPoint) {
        MethodSignature signature = (MethodSignature) joinPoint.getSignature();
        String[] paramNames = signature.getParameterNames();
        Object[] args = joinPoint.getArgs();

        for (int i = 0; i < paramNames.length; i++) {
            if ("clusterId".equals(paramNames[i]) && args[i] != null) {
                return args[i].toString();
            }
        }
        return null;
    }

    private String extractResourceName(ProceedingJoinPoint joinPoint) {
        MethodSignature signature = (MethodSignature) joinPoint.getSignature();
        String[] paramNames = signature.getParameterNames();
        Object[] args = joinPoint.getArgs();

        // Look for common resource name parameters
        String[] resourceNameParams = {"topicName", "groupId", "subject", "connectorName", "name"};

        for (int i = 0; i < paramNames.length; i++) {
            for (String resourceParam : resourceNameParams) {
                if (resourceParam.equals(paramNames[i]) && args[i] != null) {
                    return args[i].toString();
                }
            }
        }
        return null;
    }
}
