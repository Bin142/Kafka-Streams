# Kafka Management System - Development Plan

> **Tech Stack**: Java 21 + Spring Boot 3.x + React 18 + TypeScript
> **Mục tiêu**: Xây dựng hệ thống quản lý Kafka với giao diện custom, biểu đồ Mermaid, phân quyền RBAC

---

## 1. Tổng Quan Kiến Trúc

### 1.1 High-Level Architecture

```
┌─────────────────────────────────────────────────────────────────────────┐
│                           Load Balancer / Nginx                          │
└─────────────────────────────────────────────────────────────────────────┘
                    │                              │
                    ▼                              ▼
┌─────────────────────────────┐    ┌─────────────────────────────────────┐
│      Frontend (React)        │    │         Backend (Spring Boot)        │
│  ┌─────────────────────────┐ │    │  ┌─────────────────────────────────┐ │
│  │   Nginx (Static Files)  │ │    │  │      API Gateway Layer          │ │
│  │   - React SPA           │ │    │  │  - Authentication Filter        │ │
│  │   - Assets              │ │    │  │  - Rate Limiting                │ │
│  └─────────────────────────┘ │    │  │  - Request Logging              │ │
└─────────────────────────────┘    │  └─────────────────────────────────┘ │
                                    │  ┌─────────────────────────────────┐ │
                                    │  │      Service Layer              │ │
                                    │  │  - KafkaAdminService            │ │
                                    │  │  - TopicService                 │ │
                                    │  │  - ConsumerGroupService         │ │
                                    │  │  - SchemaRegistryService        │ │
                                    │  │  - UserService                  │ │
                                    │  └─────────────────────────────────┘ │
                                    │  ┌─────────────────────────────────┐ │
                                    │  │      Infrastructure Layer       │ │
                                    │  │  - KafkaClientFactory           │ │
                                    │  │  - CacheManager                 │ │
                                    │  │  - AuditLogger                  │ │
                                    │  └─────────────────────────────────┘ │
                                    └─────────────────────────────────────┘
                                                      │
                    ┌─────────────────────────────────┼─────────────────────────────────┐
                    │                                 │                                 │
                    ▼                                 ▼                                 ▼
           ┌───────────────┐                ┌───────────────┐                ┌───────────────┐
           │ Kafka Cluster │                │   PostgreSQL  │                │     Redis     │
           │ + Schema Reg  │                │  (Users/Perms)│                │   (Cache)     │
           └───────────────┘                └───────────────┘                └───────────────┘
```

### 1.2 Module Architecture (Hexagonal/Clean Architecture)

```
┌─────────────────────────────────────────────────────────────────┐
│                        Presentation Layer                        │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐             │
│  │ REST API    │  │ WebSocket   │  │ SSE         │             │
│  │ Controllers │  │ Handlers    │  │ Endpoints   │             │
│  └─────────────┘  └─────────────┘  └─────────────┘             │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│                        Application Layer                         │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐             │
│  │ Use Cases   │  │ DTOs        │  │ Mappers     │             │
│  │ (Services)  │  │             │  │             │             │
│  └─────────────┘  └─────────────┘  └─────────────┘             │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│                         Domain Layer                             │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐             │
│  │ Entities    │  │ Value       │  │ Domain      │             │
│  │             │  │ Objects     │  │ Services    │             │
│  └─────────────┘  └─────────────┘  └─────────────┘             │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│                      Infrastructure Layer                        │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐             │
│  │ Kafka       │  │ Database    │  │ External    │             │
│  │ Adapters    │  │ Repositories│  │ Services    │             │
│  └─────────────┘  └─────────────┘  └─────────────┘             │
└─────────────────────────────────────────────────────────────────┘
```

---

## 2. Cấu Trúc Project

### 2.1 Backend Structure (Spring Boot)

```
kafka-management-backend/
├── src/main/java/com/company/kafkamanagement/
│   │
│   ├── KafkaManagementApplication.java
│   │
│   ├── common/                          # Shared components
│   │   ├── config/
│   │   │   ├── SecurityConfig.java
│   │   │   ├── KafkaConfig.java
│   │   │   ├── CacheConfig.java
│   │   │   ├── WebConfig.java
│   │   │   └── OpenApiConfig.java
│   │   ├── exception/
│   │   │   ├── GlobalExceptionHandler.java
│   │   │   ├── BusinessException.java
│   │   │   ├── ResourceNotFoundException.java
│   │   │   └── UnauthorizedException.java
│   │   ├── security/
│   │   │   ├── JwtTokenProvider.java
│   │   │   ├── JwtAuthenticationFilter.java
│   │   │   ├── UserPrincipal.java
│   │   │   └── CurrentUser.java
│   │   ├── audit/
│   │   │   ├── AuditEvent.java
│   │   │   ├── AuditService.java
│   │   │   └── AuditAspect.java
│   │   └── util/
│   │       ├── PageUtils.java
│   │       └── ValidationUtils.java
│   │
│   ├── infrastructure/                  # External adapters
│   │   ├── kafka/
│   │   │   ├── KafkaClientFactory.java
│   │   │   ├── KafkaAdminClientWrapper.java
│   │   │   ├── KafkaConsumerWrapper.java
│   │   │   ├── KafkaProducerWrapper.java
│   │   │   └── SchemaRegistryClientWrapper.java
│   │   ├── persistence/
│   │   │   ├── entity/
│   │   │   │   ├── UserEntity.java
│   │   │   │   ├── RoleEntity.java
│   │   │   │   ├── PermissionEntity.java
│   │   │   │   └── ClusterConfigEntity.java
│   │   │   └── repository/
│   │   │       ├── UserRepository.java
│   │   │       ├── RoleRepository.java
│   │   │       └── ClusterConfigRepository.java
│   │   └── cache/
│   │       └── CacheService.java
│   │
│   ├── domain/                          # Business domain
│   │   ├── cluster/
│   │   │   ├── model/
│   │   │   │   ├── Cluster.java
│   │   │   │   ├── ClusterConnection.java
│   │   │   │   └── Node.java
│   │   │   └── service/
│   │   │       └── ClusterDomainService.java
│   │   ├── topic/
│   │   │   ├── model/
│   │   │   │   ├── Topic.java
│   │   │   │   ├── Partition.java
│   │   │   │   ├── TopicConfig.java
│   │   │   │   └── TopicMessage.java
│   │   │   └── service/
│   │   │       └── TopicDomainService.java
│   │   ├── consumergroup/
│   │   │   ├── model/
│   │   │   │   ├── ConsumerGroup.java
│   │   │   │   ├── ConsumerGroupMember.java
│   │   │   │   └── ConsumerGroupOffset.java
│   │   │   └── service/
│   │   │       └── ConsumerGroupDomainService.java
│   │   ├── schema/
│   │   │   ├── model/
│   │   │   │   ├── Schema.java
│   │   │   │   └── SchemaVersion.java
│   │   │   └── service/
│   │   │       └── SchemaDomainService.java
│   │   └── user/
│   │       ├── model/
│   │       │   ├── User.java
│   │       │   ├── Role.java
│   │       │   └── Permission.java
│   │       └── service/
│   │           └── UserDomainService.java
│   │
│   ├── application/                     # Use cases
│   │   ├── cluster/
│   │   │   ├── ClusterService.java
│   │   │   ├── dto/
│   │   │   │   ├── ClusterDTO.java
│   │   │   │   ├── ClusterCreateRequest.java
│   │   │   │   └── ClusterResponse.java
│   │   │   └── mapper/
│   │   │       └── ClusterMapper.java
│   │   ├── topic/
│   │   │   ├── TopicService.java
│   │   │   ├── TopicMessageService.java
│   │   │   ├── dto/
│   │   │   │   ├── TopicDTO.java
│   │   │   │   ├── TopicCreateRequest.java
│   │   │   │   ├── TopicConfigUpdateRequest.java
│   │   │   │   ├── MessageProduceRequest.java
│   │   │   │   └── MessageSearchRequest.java
│   │   │   └── mapper/
│   │   │       └── TopicMapper.java
│   │   ├── consumergroup/
│   │   │   ├── ConsumerGroupService.java
│   │   │   ├── dto/
│   │   │   │   ├── ConsumerGroupDTO.java
│   │   │   │   └── OffsetResetRequest.java
│   │   │   └── mapper/
│   │   │       └── ConsumerGroupMapper.java
│   │   ├── schema/
│   │   │   ├── SchemaService.java
│   │   │   ├── dto/
│   │   │   │   ├── SchemaDTO.java
│   │   │   │   └── SchemaCreateRequest.java
│   │   │   └── mapper/
│   │   │       └── SchemaMapper.java
│   │   ├── auth/
│   │   │   ├── AuthService.java
│   │   │   └── dto/
│   │   │       ├── LoginRequest.java
│   │   │       ├── LoginResponse.java
│   │   │       └── RefreshTokenRequest.java
│   │   └── user/
│   │       ├── UserService.java
│   │       ├── RoleService.java
│   │       └── dto/
│   │           ├── UserDTO.java
│   │           ├── UserCreateRequest.java
│   │           └── RoleDTO.java
│   │
│   └── presentation/                    # API layer
│       ├── rest/
│       │   ├── ClusterController.java
│       │   ├── TopicController.java
│       │   ├── TopicDataController.java
│       │   ├── ConsumerGroupController.java
│       │   ├── SchemaController.java
│       │   ├── NodeController.java
│       │   ├── AuthController.java
│       │   └── UserController.java
│       └── sse/
│           ├── TopicTailController.java
│           └── SearchStreamController.java
│
├── src/main/resources/
│   ├── application.yml
│   ├── application-dev.yml
│   ├── application-prod.yml
│   └── db/migration/                    # Flyway migrations
│       ├── V1__create_users_table.sql
│       ├── V2__create_roles_table.sql
│       ├── V3__create_permissions_table.sql
│       └── V4__create_cluster_configs_table.sql
│
├── src/test/java/
│   ├── unit/
│   ├── integration/
│   └── e2e/
│
├── build.gradle.kts
├── settings.gradle.kts
├── Dockerfile
└── docker-compose.yml
```

### 2.2 Frontend Structure (React)

```
kafka-management-frontend/
├── src/
│   ├── main.tsx
│   ├── App.tsx
│   ├── vite-env.d.ts
│   │
│   ├── api/                             # API layer
│   │   ├── client.ts                    # Axios instance
│   │   ├── endpoints/
│   │   │   ├── cluster.api.ts
│   │   │   ├── topic.api.ts
│   │   │   ├── consumerGroup.api.ts
│   │   │   ├── schema.api.ts
│   │   │   ├── auth.api.ts
│   │   │   └── user.api.ts
│   │   └── types/
│   │       ├── cluster.types.ts
│   │       ├── topic.types.ts
│   │       ├── consumerGroup.types.ts
│   │       ├── schema.types.ts
│   │       └── user.types.ts
│   │
│   ├── components/                      # Reusable components
│   │   ├── common/
│   │   │   ├── Layout/
│   │   │   │   ├── MainLayout.tsx
│   │   │   │   ├── Sidebar.tsx
│   │   │   │   └── Header.tsx
│   │   │   ├── Table/
│   │   │   │   ├── DataTable.tsx
│   │   │   │   └── TablePagination.tsx
│   │   │   ├── Form/
│   │   │   │   ├── FormInput.tsx
│   │   │   │   ├── FormSelect.tsx
│   │   │   │   └── FormTextArea.tsx
│   │   │   ├── Modal/
│   │   │   │   ├── ConfirmModal.tsx
│   │   │   │   └── FormModal.tsx
│   │   │   ├── Loading/
│   │   │   │   ├── PageLoading.tsx
│   │   │   │   └── TableLoading.tsx
│   │   │   └── Error/
│   │   │       ├── ErrorBoundary.tsx
│   │   │       └── ErrorMessage.tsx
│   │   │
│   │   ├── cluster/
│   │   │   ├── ClusterList.tsx
│   │   │   ├── ClusterCard.tsx
│   │   │   └── ClusterForm.tsx
│   │   │
│   │   ├── topic/
│   │   │   ├── TopicList.tsx
│   │   │   ├── TopicDetail.tsx
│   │   │   ├── TopicForm.tsx
│   │   │   ├── TopicConfigEditor.tsx
│   │   │   ├── PartitionList.tsx
│   │   │   └── MessageBrowser/
│   │   │       ├── MessageBrowser.tsx
│   │   │       ├── MessageTable.tsx
│   │   │       ├── MessageDetail.tsx
│   │   │       ├── MessageProducer.tsx
│   │   │       └── MessageSearch.tsx
│   │   │
│   │   ├── consumerGroup/
│   │   │   ├── ConsumerGroupList.tsx
│   │   │   ├── ConsumerGroupDetail.tsx
│   │   │   ├── MemberList.tsx
│   │   │   ├── OffsetTable.tsx
│   │   │   └── OffsetResetForm.tsx
│   │   │
│   │   ├── schema/
│   │   │   ├── SchemaList.tsx
│   │   │   ├── SchemaDetail.tsx
│   │   │   ├── SchemaEditor.tsx
│   │   │   └── SchemaVersionList.tsx
│   │   │
│   │   ├── mermaid/                     # Mermaid diagrams
│   │   │   ├── MermaidDiagram.tsx
│   │   │   ├── TopicFlowDiagram.tsx
│   │   │   ├── ConsumerGroupDiagram.tsx
│   │   │   └── ClusterTopologyDiagram.tsx
│   │   │
│   │   └── user/
│   │       ├── UserList.tsx
│   │       ├── UserForm.tsx
│   │       ├── RoleList.tsx
│   │       └── PermissionMatrix.tsx
│   │
│   ├── pages/                           # Page components
│   │   ├── Dashboard/
│   │   │   └── DashboardPage.tsx
│   │   ├── Cluster/
│   │   │   ├── ClusterListPage.tsx
│   │   │   └── ClusterDetailPage.tsx
│   │   ├── Topic/
│   │   │   ├── TopicListPage.tsx
│   │   │   └── TopicDetailPage.tsx
│   │   ├── ConsumerGroup/
│   │   │   ├── ConsumerGroupListPage.tsx
│   │   │   └── ConsumerGroupDetailPage.tsx
│   │   ├── Schema/
│   │   │   ├── SchemaListPage.tsx
│   │   │   └── SchemaDetailPage.tsx
│   │   ├── User/
│   │   │   ├── UserListPage.tsx
│   │   │   └── RoleListPage.tsx
│   │   ├── Auth/
│   │   │   ├── LoginPage.tsx
│   │   │   └── ForgotPasswordPage.tsx
│   │   └── Error/
│   │       ├── NotFoundPage.tsx
│   │       └── ForbiddenPage.tsx
│   │
│   ├── hooks/                           # Custom hooks
│   │   ├── useAuth.ts
│   │   ├── usePermission.ts
│   │   ├── usePagination.ts
│   │   ├── useDebounce.ts
│   │   ├── useSSE.ts
│   │   └── queries/
│   │       ├── useTopics.ts
│   │       ├── useConsumerGroups.ts
│   │       ├── useSchemas.ts
│   │       └── useUsers.ts
│   │
│   ├── stores/                          # State management
│   │   ├── authStore.ts
│   │   ├── clusterStore.ts
│   │   └── uiStore.ts
│   │
│   ├── utils/                           # Utilities
│   │   ├── constants.ts
│   │   ├── formatters.ts
│   │   ├── validators.ts
│   │   └── permissions.ts
│   │
│   ├── routes/                          # Routing
│   │   ├── index.tsx
│   │   ├── PrivateRoute.tsx
│   │   └── routes.ts
│   │
│   └── styles/                          # Global styles
│       ├── global.css
│       ├── variables.css
│       └── theme.ts
│
├── public/
│   └── favicon.ico
│
├── index.html
├── vite.config.ts
├── tsconfig.json
├── package.json
├── Dockerfile
└── nginx.conf
```



---

## 3. Dependencies & Versions

### 3.1 Backend Dependencies (build.gradle.kts)

```kotlin
plugins {
    java
    id("org.springframework.boot") version "3.3.0"
    id("io.spring.dependency-management") version "1.1.5"
}

java {
    sourceCompatibility = JavaVersion.VERSION_21
}

dependencies {
    // Spring Boot Starters
    implementation("org.springframework.boot:spring-boot-starter-web")
    implementation("org.springframework.boot:spring-boot-starter-security")
    implementation("org.springframework.boot:spring-boot-starter-data-jpa")
    implementation("org.springframework.boot:spring-boot-starter-validation")
    implementation("org.springframework.boot:spring-boot-starter-cache")
    implementation("org.springframework.boot:spring-boot-starter-actuator")
    
    // Kafka
    implementation("org.apache.kafka:kafka-clients:3.7.0")
    
    // Schema Registry
    implementation("io.confluent:kafka-schema-registry-client:7.6.0")
    implementation("io.confluent:kafka-avro-serializer:7.6.0")
    implementation("io.confluent:kafka-json-schema-serializer:7.6.0")
    implementation("io.confluent:kafka-protobuf-serializer:7.6.0")
    
    // Security
    implementation("io.jsonwebtoken:jjwt-api:0.12.5")
    runtimeOnly("io.jsonwebtoken:jjwt-impl:0.12.5")
    runtimeOnly("io.jsonwebtoken:jjwt-jackson:0.12.5")
    
    // Database
    implementation("org.postgresql:postgresql:42.7.3")
    implementation("org.flywaydb:flyway-core:10.12.0")
    implementation("org.flywaydb:flyway-database-postgresql:10.12.0")
    
    // Cache
    implementation("org.springframework.boot:spring-boot-starter-data-redis")
    implementation("com.github.ben-manes.caffeine:caffeine:3.1.8")
    
    // Utilities
    implementation("org.mapstruct:mapstruct:1.5.5.Final")
    annotationProcessor("org.mapstruct:mapstruct-processor:1.5.5.Final")
    compileOnly("org.projectlombok:lombok:1.18.32")
    annotationProcessor("org.projectlombok:lombok:1.18.32")
    annotationProcessor("org.projectlombok:lombok-mapstruct-binding:0.2.0")
    
    // API Documentation
    implementation("org.springdoc:springdoc-openapi-starter-webmvc-ui:2.5.0")
    
    // Testing
    testImplementation("org.springframework.boot:spring-boot-starter-test")
    testImplementation("org.springframework.security:spring-security-test")
    testImplementation("org.testcontainers:testcontainers:1.19.7")
    testImplementation("org.testcontainers:kafka:1.19.7")
    testImplementation("org.testcontainers:postgresql:1.19.7")
}

repositories {
    mavenCentral()
    maven { url = uri("https://packages.confluent.io/maven/") }
}
```

### 3.2 Frontend Dependencies (package.json)

```json
{
  "name": "kafka-management-frontend",
  "version": "1.0.0",
  "type": "module",
  "scripts": {
    "dev": "vite",
    "build": "tsc && vite build",
    "preview": "vite preview",
    "lint": "eslint . --ext ts,tsx --report-unused-disable-directives --max-warnings 0",
    "test": "vitest",
    "test:coverage": "vitest run --coverage"
  },
  "dependencies": {
    "react": "^18.3.1",
    "react-dom": "^18.3.1",
    "react-router-dom": "^6.23.1",
    
    "antd": "^5.17.0",
    "@ant-design/icons": "^5.3.7",
    "@ant-design/pro-components": "^2.7.1",
    
    "@tanstack/react-query": "^5.37.1",
    "axios": "^1.7.0",
    "zustand": "^4.5.2",
    
    "mermaid": "^10.9.0",
    "monaco-editor": "^0.48.0",
    "@monaco-editor/react": "^4.6.0",
    
    "dayjs": "^1.11.11",
    "lodash-es": "^4.17.21",
    "classnames": "^2.5.1"
  },
  "devDependencies": {
    "@types/react": "^18.3.2",
    "@types/react-dom": "^18.3.0",
    "@types/lodash-es": "^4.17.12",
    
    "typescript": "^5.4.5",
    "@typescript-eslint/eslint-plugin": "^7.10.0",
    "@typescript-eslint/parser": "^7.10.0",
    
    "vite": "^5.2.11",
    "@vitejs/plugin-react": "^4.3.0",
    
    "eslint": "^8.57.0",
    "eslint-plugin-react-hooks": "^4.6.2",
    "eslint-plugin-react-refresh": "^0.4.7",
    
    "vitest": "^1.6.0",
    "@testing-library/react": "^15.0.7",
    "@testing-library/jest-dom": "^6.4.5",
    
    "sass": "^1.77.2",
    "postcss": "^8.4.38",
    "autoprefixer": "^10.4.19"
  }
}
```

---

## 4. Core Implementation Examples

### 4.1 Kafka Client Factory (Infrastructure Layer)

```java
package com.company.kafkamanagement.infrastructure.kafka;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;

@Component
public class KafkaClientFactory {
    
    private final Map<String, AdminClient> adminClients = new ConcurrentHashMap<>();
    private final Map<String, KafkaProducer<byte[], byte[]>> producers = new ConcurrentHashMap<>();
    
    /**
     * Get or create AdminClient for cluster (cached)
     */
    public AdminClient getAdminClient(ClusterConnection connection) {
        return adminClients.computeIfAbsent(connection.getId(), id -> {
            Properties props = buildProperties(connection);
            return AdminClient.create(props);
        });
    }
    
    /**
     * Get or create Producer for cluster (cached)
     */
    public KafkaProducer<byte[], byte[]> getProducer(ClusterConnection connection) {
        return producers.computeIfAbsent(connection.getId(), id -> {
            Properties props = buildProperties(connection);
            return new KafkaProducer<>(props, new ByteArraySerializer(), new ByteArraySerializer());
        });
    }
    
    /**
     * Create new Consumer (not cached - each request needs own consumer)
     */
    public KafkaConsumer<byte[], byte[]> createConsumer(ClusterConnection connection) {
        Properties props = buildProperties(connection);
        return new KafkaConsumer<>(props, new ByteArrayDeserializer(), new ByteArrayDeserializer());
    }
    
    /**
     * Create new Consumer with custom properties
     */
    public KafkaConsumer<byte[], byte[]> createConsumer(ClusterConnection connection, Properties customProps) {
        Properties props = buildProperties(connection);
        props.putAll(customProps);
        return new KafkaConsumer<>(props, new ByteArrayDeserializer(), new ByteArrayDeserializer());
    }
    
    private Properties buildProperties(ClusterConnection connection) {
        Properties props = new Properties();
        props.put("bootstrap.servers", connection.getBootstrapServers());
        
        // Security configuration
        if (connection.getSecurityProtocol() != null) {
            props.put("security.protocol", connection.getSecurityProtocol());
        }
        
        if (connection.getSaslMechanism() != null) {
            props.put("sasl.mechanism", connection.getSaslMechanism());
            props.put("sasl.jaas.config", connection.getSaslJaasConfig());
        }
        
        // SSL configuration
        if (connection.getSslTruststoreLocation() != null) {
            props.put("ssl.truststore.location", connection.getSslTruststoreLocation());
            props.put("ssl.truststore.password", connection.getSslTruststorePassword());
        }
        
        if (connection.getSslKeystoreLocation() != null) {
            props.put("ssl.keystore.location", connection.getSslKeystoreLocation());
            props.put("ssl.keystore.password", connection.getSslKeystorePassword());
            props.put("ssl.key.password", connection.getSslKeyPassword());
        }
        
        // Additional properties
        if (connection.getAdditionalProperties() != null) {
            props.putAll(connection.getAdditionalProperties());
        }
        
        return props;
    }
    
    /**
     * Close all clients for a cluster
     */
    public void closeClients(String clusterId) {
        AdminClient adminClient = adminClients.remove(clusterId);
        if (adminClient != null) {
            adminClient.close();
        }
        
        KafkaProducer<byte[], byte[]> producer = producers.remove(clusterId);
        if (producer != null) {
            producer.close();
        }
    }
    
    /**
     * Close all clients (shutdown)
     */
    public void closeAll() {
        adminClients.values().forEach(AdminClient::close);
        adminClients.clear();
        
        producers.values().forEach(KafkaProducer::close);
        producers.clear();
    }
}
```

### 4.2 Topic Service (Application Layer)

```java
package com.company.kafkamanagement.application.topic;

import com.company.kafkamanagement.application.topic.dto.*;
import com.company.kafkamanagement.application.topic.mapper.TopicMapper;
import com.company.kafkamanagement.common.exception.ResourceNotFoundException;
import com.company.kafkamanagement.domain.topic.model.Topic;
import com.company.kafkamanagement.infrastructure.kafka.KafkaAdminClientWrapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.cache.annotation.CacheEvict;
import org.springframework.cache.annotation.Cacheable;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map;

@Slf4j
@Service
@RequiredArgsConstructor
public class TopicService {
    
    private final KafkaAdminClientWrapper kafkaAdmin;
    private final TopicMapper topicMapper;
    
    /**
     * List all topics with pagination and filtering
     */
    @Cacheable(value = "topics", key = "#clusterId + '-' + #pageable.pageNumber + '-' + #search")
    public Page<TopicDTO> listTopics(String clusterId, Pageable pageable, 
                                      String search, TopicListView view) {
        log.debug("Listing topics for cluster: {}, search: {}, view: {}", clusterId, search, view);
        
        List<Topic> topics = kafkaAdmin.listTopics(clusterId, view);
        
        // Filter by search
        if (search != null && !search.isBlank()) {
            topics = topics.stream()
                .filter(t -> t.getName().toLowerCase().contains(search.toLowerCase()))
                .toList();
        }
        
        // Convert to DTOs with pagination
        return topicMapper.toPagedDTO(topics, pageable);
    }
    
    /**
     * Get topic details
     */
    @Cacheable(value = "topic", key = "#clusterId + '-' + #topicName")
    public TopicDetailDTO getTopic(String clusterId, String topicName) {
        log.debug("Getting topic: {} from cluster: {}", topicName, clusterId);
        
        Topic topic = kafkaAdmin.describeTopic(clusterId, topicName)
            .orElseThrow(() -> new ResourceNotFoundException("Topic", topicName));
        
        return topicMapper.toDetailDTO(topic);
    }
    
    /**
     * Create new topic
     */
    @CacheEvict(value = "topics", allEntries = true)
    public TopicDTO createTopic(String clusterId, TopicCreateRequest request) {
        log.info("Creating topic: {} in cluster: {}", request.getName(), clusterId);
        
        kafkaAdmin.createTopic(
            clusterId,
            request.getName(),
            request.getPartitions(),
            request.getReplicationFactor(),
            request.getConfigs()
        );
        
        // Wait for topic to be created and return details
        Topic topic = kafkaAdmin.describeTopic(clusterId, request.getName())
            .orElseThrow(() -> new IllegalStateException("Topic creation failed"));
        
        return topicMapper.toDTO(topic);
    }
    
    /**
     * Delete topic
     */
    @CacheEvict(value = {"topics", "topic"}, allEntries = true)
    public void deleteTopic(String clusterId, String topicName) {
        log.info("Deleting topic: {} from cluster: {}", topicName, clusterId);
        kafkaAdmin.deleteTopic(clusterId, topicName);
    }
    
    /**
     * Increase partitions
     */
    @CacheEvict(value = "topic", key = "#clusterId + '-' + #topicName")
    public void increasePartitions(String clusterId, String topicName, int newPartitionCount) {
        log.info("Increasing partitions for topic: {} to: {}", topicName, newPartitionCount);
        kafkaAdmin.increasePartitions(clusterId, topicName, newPartitionCount);
    }
    
    /**
     * Get topic configs
     */
    public List<TopicConfigDTO> getTopicConfigs(String clusterId, String topicName) {
        log.debug("Getting configs for topic: {}", topicName);
        return kafkaAdmin.describeTopicConfigs(clusterId, topicName).stream()
            .map(topicMapper::toConfigDTO)
            .toList();
    }
    
    /**
     * Update topic configs
     */
    @CacheEvict(value = "topic", key = "#clusterId + '-' + #topicName")
    public void updateTopicConfigs(String clusterId, String topicName, 
                                    Map<String, String> configs) {
        log.info("Updating configs for topic: {}", topicName);
        kafkaAdmin.alterTopicConfigs(clusterId, topicName, configs);
    }
}
```

### 4.3 Permission System (Dynamic RBAC via Database)

> **Lưu ý quan trọng**: Hệ thống phân quyền được quản lý **hoàn toàn qua Web UI** và lưu trong **Database**, KHÔNG phải hardcode trong YAML hay code.

#### 4.3.1 Mô Hình Phân Quyền

```
┌─────────────────────────────────────────────────────────────────────────┐
│                         RBAC Model (Database)                            │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                          │
│   ┌──────────┐       ┌──────────┐       ┌──────────────┐                │
│   │   User   │──M:N──│   Role   │──M:N──│  Permission  │                │
│   └──────────┘       └──────────┘       └──────────────┘                │
│        │                   │                    │                        │
│        │                   │                    ├── resource (enum)      │
│        ├── username        ├── name             ├── action (enum)        │
│        ├── email           ├── description      ├── resource_pattern     │
│        ├── password        └── is_system        └── cluster_ids[]        │
│        └── is_admin                                                      │
│                                                                          │
└─────────────────────────────────────────────────────────────────────────┘

Ví dụ:
- User "john" có Role "developer"
- Role "developer" có Permission: {resource: TOPIC, action: READ, pattern: "dev-*"}
- → john chỉ được READ các topics bắt đầu bằng "dev-"
```

#### 4.3.2 Enums (Định nghĩa trong code, giá trị lưu DB)

```java
package com.company.kafkamanagement.domain.user.model;

import lombok.Getter;

/**
 * Resource types - định nghĩa các loại tài nguyên có thể phân quyền
 * Giá trị được lưu dưới dạng String trong database
 */
@Getter
public enum Resource {
    CLUSTER("cluster"),
    TOPIC("topic"),
    TOPIC_DATA("topic_data"),
    CONSUMER_GROUP("consumer_group"),
    SCHEMA("schema"),
    NODE("node"),
    ACL("acl"),
    USER("user"),
    ROLE("role");
    
    private final String value;
    
    Resource(String value) {
        this.value = value;
    }
}

/**
 * Action types - định nghĩa các hành động có thể thực hiện
 */
@Getter
public enum Action {
    READ("read"),
    CREATE("create"),
    UPDATE("update"),
    DELETE("delete"),
    MANAGE("manage");  // Full access - bao gồm tất cả actions
    
    private final String value;
    
    Action(String value) {
        this.value = value;
    }
}
```

#### 4.3.3 Database Entities

```java
package com.company.kafkamanagement.domain.user.model;

import jakarta.persistence.*;
import lombok.*;
import java.time.LocalDateTime;
import java.util.HashSet;
import java.util.Set;

/**
 * User Entity - Lưu thông tin người dùng
 */
@Entity
@Table(name = "users")
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class User {
    
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;
    
    @Column(nullable = false, unique = true, length = 50)
    private String username;
    
    @Column(nullable = false, unique = true, length = 100)
    private String email;
    
    @Column(name = "password_hash", nullable = false)
    private String passwordHash;
    
    @Column(name = "full_name", length = 100)
    private String fullName;
    
    @Column(name = "is_admin")
    private boolean isAdmin = false;
    
    @Column(name = "is_active")
    private boolean isActive = true;
    
    @Column(name = "created_at")
    private LocalDateTime createdAt;
    
    @Column(name = "updated_at")
    private LocalDateTime updatedAt;
    
    @Column(name = "last_login_at")
    private LocalDateTime lastLoginAt;
    
    /**
     * User có nhiều Roles (Many-to-Many)
     * Quản lý qua Web UI
     */
    @ManyToMany(fetch = FetchType.EAGER)
    @JoinTable(
        name = "user_roles",
        joinColumns = @JoinColumn(name = "user_id"),
        inverseJoinColumns = @JoinColumn(name = "role_id")
    )
    private Set<Role> roles = new HashSet<>();
    
    /**
     * Lấy tất cả permissions từ các roles
     */
    public Set<Permission> getAllPermissions() {
        Set<Permission> permissions = new HashSet<>();
        for (Role role : roles) {
            permissions.addAll(role.getPermissions());
        }
        return permissions;
    }
}

/**
 * Role Entity - Lưu thông tin vai trò
 * Tạo/Sửa/Xóa qua Web UI
 */
@Entity
@Table(name = "roles")
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class Role {
    
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;
    
    @Column(nullable = false, unique = true, length = 50)
    private String name;
    
    @Column(length = 255)
    private String description;
    
    /**
     * System roles không thể xóa (admin, viewer)
     */
    @Column(name = "is_system")
    private boolean isSystem = false;
    
    @Column(name = "created_at")
    private LocalDateTime createdAt;
    
    @Column(name = "updated_at")
    private LocalDateTime updatedAt;
    
    /**
     * Role có nhiều Permissions (Many-to-Many)
     * Quản lý qua Web UI
     */
    @ManyToMany(fetch = FetchType.EAGER)
    @JoinTable(
        name = "role_permissions",
        joinColumns = @JoinColumn(name = "role_id"),
        inverseJoinColumns = @JoinColumn(name = "permission_id")
    )
    private Set<Permission> permissions = new HashSet<>();
}

/**
 * Permission Entity - Lưu thông tin quyền hạn
 * Tạo/Sửa/Xóa qua Web UI
 */
@Entity
@Table(name = "permissions")
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class Permission {
    
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;
    
    /**
     * Loại tài nguyên (TOPIC, CONSUMER_GROUP, etc.)
     */
    @Enumerated(EnumType.STRING)
    @Column(nullable = false, length = 50)
    private Resource resource;
    
    /**
     * Hành động được phép (READ, CREATE, UPDATE, DELETE, MANAGE)
     */
    @Enumerated(EnumType.STRING)
    @Column(nullable = false, length = 50)
    private Action action;
    
    /**
     * Pattern để match tên resource
     * Ví dụ: "dev-*" chỉ match topics bắt đầu bằng "dev-"
     * null = tất cả resources
     */
    @Column(name = "resource_pattern", length = 255)
    private String resourcePattern;
    
    /**
     * Danh sách cluster IDs mà permission này áp dụng
     * Empty = tất cả clusters
     */
    @ElementCollection(fetch = FetchType.EAGER)
    @CollectionTable(
        name = "permission_clusters", 
        joinColumns = @JoinColumn(name = "permission_id")
    )
    @Column(name = "cluster_id")
    private Set<String> clusterIds = new HashSet<>();
    
    @Column(name = "created_at")
    private LocalDateTime createdAt;
    
    /**
     * Mô tả permission (hiển thị trên UI)
     */
    @Column(length = 255)
    private String description;
    
    /**
     * Check if this permission matches the given resource
     */
    public boolean matches(String clusterId, Resource resource, Action action, String resourceName) {
        // Check resource type
        if (this.resource != resource) {
            return false;
        }
        
        // Check action (MANAGE includes all actions)
        if (this.action != Action.MANAGE && this.action != action) {
            return false;
        }
        
        // Check cluster (empty = all clusters)
        if (!clusterIds.isEmpty() && !clusterIds.contains(clusterId)) {
            return false;
        }
        
        // Check resource pattern (null = all resources)
        if (resourcePattern != null && resourceName != null) {
            return matchesPattern(resourceName, resourcePattern);
        }
        
        return true;
    }
    
    private boolean matchesPattern(String name, String pattern) {
        // Convert glob pattern to regex
        // "dev-*" -> "dev-.*"
        // "orders-?" -> "orders-."
        String regex = pattern
            .replace(".", "\\.")
            .replace("*", ".*")
            .replace("?", ".");
        return name.matches(regex);
    }
    
    /**
     * Tạo description tự động cho UI
     */
    public String getDisplayDescription() {
        StringBuilder sb = new StringBuilder();
        sb.append(action.getValue().toUpperCase())
          .append(" ")
          .append(resource.getValue());
        
        if (resourcePattern != null) {
            sb.append(" matching '").append(resourcePattern).append("'");
        }
        
        if (!clusterIds.isEmpty()) {
            sb.append(" on clusters: ").append(String.join(", ", clusterIds));
        }
        
        return sb.toString();
    }
}
```

```java
package com.company.kafkamanagement.common.security;

import com.company.kafkamanagement.domain.user.model.Action;
import com.company.kafkamanagement.domain.user.model.Permission;
import com.company.kafkamanagement.domain.user.model.Resource;
import com.company.kafkamanagement.domain.user.model.User;
import lombok.RequiredArgsConstructor;
import org.springframework.security.access.AccessDeniedException;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
public class PermissionChecker {
    
    /**
     * Check if current user has permission
     */
    public void checkPermission(User user, String clusterId, Resource resource, Action action) {
        checkPermission(user, clusterId, resource, action, null);
    }
    
    /**
     * Check if current user has permission for specific resource
     */
    public void checkPermission(User user, String clusterId, Resource resource, 
                                 Action action, String resourceName) {
        if (!hasPermission(user, clusterId, resource, action, resourceName)) {
            throw new AccessDeniedException(
                String.format("User %s does not have %s permission on %s", 
                    user.getUsername(), action, resource)
            );
        }
    }
    
    /**
     * Check if user has permission (without throwing)
     */
    public boolean hasPermission(User user, String clusterId, Resource resource, 
                                  Action action, String resourceName) {
        // Admin has all permissions
        if (user.isAdmin()) {
            return true;
        }
        
        // Check user's roles and permissions
        return user.getRoles().stream()
            .flatMap(role -> role.getPermissions().stream())
            .anyMatch(permission -> permission.matches(clusterId, resource, action, resourceName));
    }
}
```

---

### 4.4 Role & Permission Management APIs

> **Quan trọng**: Các API này cho phép quản lý phân quyền **trực tiếp qua Web UI**

#### 4.4.1 Role Service

```java
package com.company.kafkamanagement.application.user;

import com.company.kafkamanagement.application.user.dto.*;
import com.company.kafkamanagement.common.exception.BusinessException;
import com.company.kafkamanagement.common.exception.ResourceNotFoundException;
import com.company.kafkamanagement.domain.user.model.Permission;
import com.company.kafkamanagement.domain.user.model.Role;
import com.company.kafkamanagement.infrastructure.persistence.repository.PermissionRepository;
import com.company.kafkamanagement.infrastructure.persistence.repository.RoleRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDateTime;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

@Slf4j
@Service
@RequiredArgsConstructor
public class RoleService {
    
    private final RoleRepository roleRepository;
    private final PermissionRepository permissionRepository;
    
    /**
     * Lấy danh sách tất cả roles
     */
    public Page<RoleDTO> listRoles(Pageable pageable, String search) {
        Page<Role> roles;
        if (search != null && !search.isBlank()) {
            roles = roleRepository.findByNameContainingIgnoreCase(search, pageable);
        } else {
            roles = roleRepository.findAll(pageable);
        }
        return roles.map(this::toDTO);
    }
    
    /**
     * Lấy chi tiết role với permissions
     */
    public RoleDetailDTO getRole(Long roleId) {
        Role role = roleRepository.findById(roleId)
            .orElseThrow(() -> new ResourceNotFoundException("Role", roleId));
        return toDetailDTO(role);
    }
    
    /**
     * Tạo role mới qua Web UI
     */
    @Transactional
    public RoleDTO createRole(RoleCreateRequest request) {
        // Check duplicate name
        if (roleRepository.existsByName(request.getName())) {
            throw new BusinessException("Role with name '" + request.getName() + "' already exists");
        }
        
        Role role = Role.builder()
            .name(request.getName())
            .description(request.getDescription())
            .isSystem(false)  // User-created roles are not system roles
            .createdAt(LocalDateTime.now())
            .updatedAt(LocalDateTime.now())
            .permissions(new HashSet<>())
            .build();
        
        // Add permissions if provided
        if (request.getPermissionIds() != null && !request.getPermissionIds().isEmpty()) {
            Set<Permission> permissions = new HashSet<>(
                permissionRepository.findAllById(request.getPermissionIds())
            );
            role.setPermissions(permissions);
        }
        
        role = roleRepository.save(role);
        log.info("Created new role: {}", role.getName());
        
        return toDTO(role);
    }
    
    /**
     * Cập nhật role qua Web UI
     */
    @Transactional
    public RoleDTO updateRole(Long roleId, RoleUpdateRequest request) {
        Role role = roleRepository.findById(roleId)
            .orElseThrow(() -> new ResourceNotFoundException("Role", roleId));
        
        // System roles cannot be renamed
        if (role.isSystem() && !role.getName().equals(request.getName())) {
            throw new BusinessException("Cannot rename system role");
        }
        
        role.setName(request.getName());
        role.setDescription(request.getDescription());
        role.setUpdatedAt(LocalDateTime.now());
        
        role = roleRepository.save(role);
        log.info("Updated role: {}", role.getName());
        
        return toDTO(role);
    }
    
    /**
     * Xóa role qua Web UI
     */
    @Transactional
    public void deleteRole(Long roleId) {
        Role role = roleRepository.findById(roleId)
            .orElseThrow(() -> new ResourceNotFoundException("Role", roleId));
        
        // Cannot delete system roles
        if (role.isSystem()) {
            throw new BusinessException("Cannot delete system role: " + role.getName());
        }
        
        roleRepository.delete(role);
        log.info("Deleted role: {}", role.getName());
    }
    
    /**
     * Gán permissions cho role qua Web UI
     */
    @Transactional
    public RoleDetailDTO assignPermissions(Long roleId, List<Long> permissionIds) {
        Role role = roleRepository.findById(roleId)
            .orElseThrow(() -> new ResourceNotFoundException("Role", roleId));
        
        Set<Permission> permissions = new HashSet<>(
            permissionRepository.findAllById(permissionIds)
        );
        
        role.setPermissions(permissions);
        role.setUpdatedAt(LocalDateTime.now());
        role = roleRepository.save(role);
        
        log.info("Assigned {} permissions to role: {}", permissions.size(), role.getName());
        
        return toDetailDTO(role);
    }
    
    /**
     * Thêm permission vào role
     */
    @Transactional
    public RoleDetailDTO addPermission(Long roleId, Long permissionId) {
        Role role = roleRepository.findById(roleId)
            .orElseThrow(() -> new ResourceNotFoundException("Role", roleId));
        
        Permission permission = permissionRepository.findById(permissionId)
            .orElseThrow(() -> new ResourceNotFoundException("Permission", permissionId));
        
        role.getPermissions().add(permission);
        role.setUpdatedAt(LocalDateTime.now());
        role = roleRepository.save(role);
        
        log.info("Added permission {} to role: {}", permission.getId(), role.getName());
        
        return toDetailDTO(role);
    }
    
    /**
     * Xóa permission khỏi role
     */
    @Transactional
    public RoleDetailDTO removePermission(Long roleId, Long permissionId) {
        Role role = roleRepository.findById(roleId)
            .orElseThrow(() -> new ResourceNotFoundException("Role", roleId));
        
        role.getPermissions().removeIf(p -> p.getId().equals(permissionId));
        role.setUpdatedAt(LocalDateTime.now());
        role = roleRepository.save(role);
        
        log.info("Removed permission {} from role: {}", permissionId, role.getName());
        
        return toDetailDTO(role);
    }
    
    private RoleDTO toDTO(Role role) {
        return RoleDTO.builder()
            .id(role.getId())
            .name(role.getName())
            .description(role.getDescription())
            .isSystem(role.isSystem())
            .permissionCount(role.getPermissions().size())
            .createdAt(role.getCreatedAt())
            .updatedAt(role.getUpdatedAt())
            .build();
    }
    
    private RoleDetailDTO toDetailDTO(Role role) {
        return RoleDetailDTO.builder()
            .id(role.getId())
            .name(role.getName())
            .description(role.getDescription())
            .isSystem(role.isSystem())
            .permissions(role.getPermissions().stream()
                .map(this::toPermissionDTO)
                .toList())
            .createdAt(role.getCreatedAt())
            .updatedAt(role.getUpdatedAt())
            .build();
    }
    
    private PermissionDTO toPermissionDTO(Permission permission) {
        return PermissionDTO.builder()
            .id(permission.getId())
            .resource(permission.getResource().getValue())
            .action(permission.getAction().getValue())
            .resourcePattern(permission.getResourcePattern())
            .clusterIds(permission.getClusterIds())
            .description(permission.getDisplayDescription())
            .build();
    }
}
```

#### 4.4.2 Permission Service

```java
package com.company.kafkamanagement.application.user;

import com.company.kafkamanagement.application.user.dto.*;
import com.company.kafkamanagement.common.exception.ResourceNotFoundException;
import com.company.kafkamanagement.domain.user.model.Action;
import com.company.kafkamanagement.domain.user.model.Permission;
import com.company.kafkamanagement.domain.user.model.Resource;
import com.company.kafkamanagement.infrastructure.persistence.repository.PermissionRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;

@Slf4j
@Service
@RequiredArgsConstructor
public class PermissionService {
    
    private final PermissionRepository permissionRepository;
    
    /**
     * Lấy danh sách tất cả permissions
     */
    public Page<PermissionDTO> listPermissions(Pageable pageable, String resourceFilter) {
        Page<Permission> permissions;
        if (resourceFilter != null && !resourceFilter.isBlank()) {
            Resource resource = Resource.valueOf(resourceFilter.toUpperCase());
            permissions = permissionRepository.findByResource(resource, pageable);
        } else {
            permissions = permissionRepository.findAll(pageable);
        }
        return permissions.map(this::toDTO);
    }
    
    /**
     * Lấy danh sách resource types (cho dropdown trên UI)
     */
    public List<ResourceTypeDTO> getResourceTypes() {
        return Arrays.stream(Resource.values())
            .map(r -> new ResourceTypeDTO(r.name(), r.getValue()))
            .toList();
    }
    
    /**
     * Lấy danh sách action types (cho dropdown trên UI)
     */
    public List<ActionTypeDTO> getActionTypes() {
        return Arrays.stream(Action.values())
            .map(a -> new ActionTypeDTO(a.name(), a.getValue()))
            .toList();
    }
    
    /**
     * Tạo permission mới qua Web UI
     */
    @Transactional
    public PermissionDTO createPermission(PermissionCreateRequest request) {
        Permission permission = Permission.builder()
            .resource(Resource.valueOf(request.getResource().toUpperCase()))
            .action(Action.valueOf(request.getAction().toUpperCase()))
            .resourcePattern(request.getResourcePattern())
            .clusterIds(request.getClusterIds() != null ? 
                new HashSet<>(request.getClusterIds()) : new HashSet<>())
            .description(request.getDescription())
            .createdAt(LocalDateTime.now())
            .build();
        
        permission = permissionRepository.save(permission);
        log.info("Created new permission: {} {} on {}", 
            permission.getAction(), permission.getResource(), permission.getResourcePattern());
        
        return toDTO(permission);
    }
    
    /**
     * Cập nhật permission qua Web UI
     */
    @Transactional
    public PermissionDTO updatePermission(Long permissionId, PermissionUpdateRequest request) {
        Permission permission = permissionRepository.findById(permissionId)
            .orElseThrow(() -> new ResourceNotFoundException("Permission", permissionId));
        
        permission.setResource(Resource.valueOf(request.getResource().toUpperCase()));
        permission.setAction(Action.valueOf(request.getAction().toUpperCase()));
        permission.setResourcePattern(request.getResourcePattern());
        permission.setClusterIds(request.getClusterIds() != null ? 
            new HashSet<>(request.getClusterIds()) : new HashSet<>());
        permission.setDescription(request.getDescription());
        
        permission = permissionRepository.save(permission);
        log.info("Updated permission: {}", permission.getId());
        
        return toDTO(permission);
    }
    
    /**
     * Xóa permission qua Web UI
     */
    @Transactional
    public void deletePermission(Long permissionId) {
        Permission permission = permissionRepository.findById(permissionId)
            .orElseThrow(() -> new ResourceNotFoundException("Permission", permissionId));
        
        permissionRepository.delete(permission);
        log.info("Deleted permission: {}", permissionId);
    }
    
    private PermissionDTO toDTO(Permission permission) {
        return PermissionDTO.builder()
            .id(permission.getId())
            .resource(permission.getResource().getValue())
            .action(permission.getAction().getValue())
            .resourcePattern(permission.getResourcePattern())
            .clusterIds(permission.getClusterIds())
            .description(permission.getDisplayDescription())
            .createdAt(permission.getCreatedAt())
            .build();
    }
}
```

#### 4.4.3 User Service (Gán Role cho User)

```java
package com.company.kafkamanagement.application.user;

// ... imports

@Slf4j
@Service
@RequiredArgsConstructor
public class UserService {
    
    private final UserRepository userRepository;
    private final RoleRepository roleRepository;
    private final PasswordEncoder passwordEncoder;
    
    /**
     * Gán roles cho user qua Web UI
     */
    @Transactional
    public UserDetailDTO assignRoles(Long userId, List<Long> roleIds) {
        User user = userRepository.findById(userId)
            .orElseThrow(() -> new ResourceNotFoundException("User", userId));
        
        Set<Role> roles = new HashSet<>(roleRepository.findAllById(roleIds));
        
        user.setRoles(roles);
        user.setUpdatedAt(LocalDateTime.now());
        user = userRepository.save(user);
        
        log.info("Assigned {} roles to user: {}", roles.size(), user.getUsername());
        
        return toDetailDTO(user);
    }
    
    /**
     * Thêm role cho user
     */
    @Transactional
    public UserDetailDTO addRole(Long userId, Long roleId) {
        User user = userRepository.findById(userId)
            .orElseThrow(() -> new ResourceNotFoundException("User", userId));
        
        Role role = roleRepository.findById(roleId)
            .orElseThrow(() -> new ResourceNotFoundException("Role", roleId));
        
        user.getRoles().add(role);
        user.setUpdatedAt(LocalDateTime.now());
        user = userRepository.save(user);
        
        log.info("Added role {} to user: {}", role.getName(), user.getUsername());
        
        return toDetailDTO(user);
    }
    
    /**
     * Xóa role khỏi user
     */
    @Transactional
    public UserDetailDTO removeRole(Long userId, Long roleId) {
        User user = userRepository.findById(userId)
            .orElseThrow(() -> new ResourceNotFoundException("User", userId));
        
        user.getRoles().removeIf(r -> r.getId().equals(roleId));
        user.setUpdatedAt(LocalDateTime.now());
        user = userRepository.save(user);
        
        log.info("Removed role {} from user: {}", roleId, user.getUsername());
        
        return toDetailDTO(user);
    }
    
    /**
     * Lấy tất cả permissions của user (từ tất cả roles)
     */
    public List<PermissionDTO> getUserPermissions(Long userId) {
        User user = userRepository.findById(userId)
            .orElseThrow(() -> new ResourceNotFoundException("User", userId));
        
        return user.getAllPermissions().stream()
            .map(this::toPermissionDTO)
            .distinct()
            .toList();
    }
    
    // ... other methods
}
```

#### 4.4.4 REST Controllers

```java
package com.company.kafkamanagement.presentation.rest;

import com.company.kafkamanagement.application.user.*;
import com.company.kafkamanagement.application.user.dto.*;
import com.company.kafkamanagement.common.security.RequirePermission;
import com.company.kafkamanagement.domain.user.model.Action;
import com.company.kafkamanagement.domain.user.model.Resource;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;

/**
 * Role Management Controller
 * Quản lý roles qua Web UI
 */
@RestController
@RequestMapping("/api/v1/roles")
@RequiredArgsConstructor
@Tag(name = "Roles", description = "Role management APIs")
public class RoleController {
    
    private final RoleService roleService;
    
    @GetMapping
    @RequirePermission(resource = Resource.ROLE, action = Action.READ)
    @Operation(summary = "List all roles")
    public ResponseEntity<Page<RoleDTO>> listRoles(
            @RequestParam(required = false) String search,
            Pageable pageable) {
        return ResponseEntity.ok(roleService.listRoles(pageable, search));
    }
    
    @GetMapping("/{roleId}")
    @RequirePermission(resource = Resource.ROLE, action = Action.READ)
    @Operation(summary = "Get role details with permissions")
    public ResponseEntity<RoleDetailDTO> getRole(@PathVariable Long roleId) {
        return ResponseEntity.ok(roleService.getRole(roleId));
    }
    
    @PostMapping
    @RequirePermission(resource = Resource.ROLE, action = Action.CREATE)
    @Operation(summary = "Create new role")
    public ResponseEntity<RoleDTO> createRole(@Valid @RequestBody RoleCreateRequest request) {
        return ResponseEntity.status(HttpStatus.CREATED)
            .body(roleService.createRole(request));
    }
    
    @PutMapping("/{roleId}")
    @RequirePermission(resource = Resource.ROLE, action = Action.UPDATE)
    @Operation(summary = "Update role")
    public ResponseEntity<RoleDTO> updateRole(
            @PathVariable Long roleId,
            @Valid @RequestBody RoleUpdateRequest request) {
        return ResponseEntity.ok(roleService.updateRole(roleId, request));
    }
    
    @DeleteMapping("/{roleId}")
    @RequirePermission(resource = Resource.ROLE, action = Action.DELETE)
    @Operation(summary = "Delete role")
    public ResponseEntity<Void> deleteRole(@PathVariable Long roleId) {
        roleService.deleteRole(roleId);
        return ResponseEntity.noContent().build();
    }
    
    @PutMapping("/{roleId}/permissions")
    @RequirePermission(resource = Resource.ROLE, action = Action.UPDATE)
    @Operation(summary = "Assign permissions to role")
    public ResponseEntity<RoleDetailDTO> assignPermissions(
            @PathVariable Long roleId,
            @RequestBody List<Long> permissionIds) {
        return ResponseEntity.ok(roleService.assignPermissions(roleId, permissionIds));
    }
    
    @PostMapping("/{roleId}/permissions/{permissionId}")
    @RequirePermission(resource = Resource.ROLE, action = Action.UPDATE)
    @Operation(summary = "Add permission to role")
    public ResponseEntity<RoleDetailDTO> addPermission(
            @PathVariable Long roleId,
            @PathVariable Long permissionId) {
        return ResponseEntity.ok(roleService.addPermission(roleId, permissionId));
    }
    
    @DeleteMapping("/{roleId}/permissions/{permissionId}")
    @RequirePermission(resource = Resource.ROLE, action = Action.UPDATE)
    @Operation(summary = "Remove permission from role")
    public ResponseEntity<RoleDetailDTO> removePermission(
            @PathVariable Long roleId,
            @PathVariable Long permissionId) {
        return ResponseEntity.ok(roleService.removePermission(roleId, permissionId));
    }
}

/**
 * Permission Management Controller
 * Quản lý permissions qua Web UI
 */
@RestController
@RequestMapping("/api/v1/permissions")
@RequiredArgsConstructor
@Tag(name = "Permissions", description = "Permission management APIs")
public class PermissionController {
    
    private final PermissionService permissionService;
    
    @GetMapping
    @RequirePermission(resource = Resource.ROLE, action = Action.READ)
    @Operation(summary = "List all permissions")
    public ResponseEntity<Page<PermissionDTO>> listPermissions(
            @RequestParam(required = false) String resource,
            Pageable pageable) {
        return ResponseEntity.ok(permissionService.listPermissions(pageable, resource));
    }
    
    @GetMapping("/resource-types")
    @Operation(summary = "Get available resource types for dropdown")
    public ResponseEntity<List<ResourceTypeDTO>> getResourceTypes() {
        return ResponseEntity.ok(permissionService.getResourceTypes());
    }
    
    @GetMapping("/action-types")
    @Operation(summary = "Get available action types for dropdown")
    public ResponseEntity<List<ActionTypeDTO>> getActionTypes() {
        return ResponseEntity.ok(permissionService.getActionTypes());
    }
    
    @PostMapping
    @RequirePermission(resource = Resource.ROLE, action = Action.CREATE)
    @Operation(summary = "Create new permission")
    public ResponseEntity<PermissionDTO> createPermission(
            @Valid @RequestBody PermissionCreateRequest request) {
        return ResponseEntity.status(HttpStatus.CREATED)
            .body(permissionService.createPermission(request));
    }
    
    @PutMapping("/{permissionId}")
    @RequirePermission(resource = Resource.ROLE, action = Action.UPDATE)
    @Operation(summary = "Update permission")
    public ResponseEntity<PermissionDTO> updatePermission(
            @PathVariable Long permissionId,
            @Valid @RequestBody PermissionUpdateRequest request) {
        return ResponseEntity.ok(permissionService.updatePermission(permissionId, request));
    }
    
    @DeleteMapping("/{permissionId}")
    @RequirePermission(resource = Resource.ROLE, action = Action.DELETE)
    @Operation(summary = "Delete permission")
    public ResponseEntity<Void> deletePermission(@PathVariable Long permissionId) {
        permissionService.deletePermission(permissionId);
        return ResponseEntity.noContent().build();
    }
}

/**
 * User Role Assignment Controller
 */
@RestController
@RequestMapping("/api/v1/users")
@RequiredArgsConstructor
@Tag(name = "Users", description = "User management APIs")
public class UserController {
    
    private final UserService userService;
    
    // ... other user endpoints
    
    @PutMapping("/{userId}/roles")
    @RequirePermission(resource = Resource.USER, action = Action.UPDATE)
    @Operation(summary = "Assign roles to user")
    public ResponseEntity<UserDetailDTO> assignRoles(
            @PathVariable Long userId,
            @RequestBody List<Long> roleIds) {
        return ResponseEntity.ok(userService.assignRoles(userId, roleIds));
    }
    
    @PostMapping("/{userId}/roles/{roleId}")
    @RequirePermission(resource = Resource.USER, action = Action.UPDATE)
    @Operation(summary = "Add role to user")
    public ResponseEntity<UserDetailDTO> addRole(
            @PathVariable Long userId,
            @PathVariable Long roleId) {
        return ResponseEntity.ok(userService.addRole(userId, roleId));
    }
    
    @DeleteMapping("/{userId}/roles/{roleId}")
    @RequirePermission(resource = Resource.USER, action = Action.UPDATE)
    @Operation(summary = "Remove role from user")
    public ResponseEntity<UserDetailDTO> removeRole(
            @PathVariable Long userId,
            @PathVariable Long roleId) {
        return ResponseEntity.ok(userService.removeRole(userId, roleId));
    }
    
    @GetMapping("/{userId}/permissions")
    @RequirePermission(resource = Resource.USER, action = Action.READ)
    @Operation(summary = "Get all permissions of user")
    public ResponseEntity<List<PermissionDTO>> getUserPermissions(@PathVariable Long userId) {
        return ResponseEntity.ok(userService.getUserPermissions(userId));
    }
}
```

### 4.4 Security Annotation

```java
package com.company.kafkamanagement.common.security;

import com.company.kafkamanagement.domain.user.model.Action;
import com.company.kafkamanagement.domain.user.model.Resource;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Target({ElementType.METHOD, ElementType.TYPE})
@Retention(RetentionPolicy.RUNTIME)
public @interface RequirePermission {
    Resource resource();
    Action action();
}
```

```java
package com.company.kafkamanagement.common.security;

import com.company.kafkamanagement.domain.user.model.User;
import lombok.RequiredArgsConstructor;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.reflect.MethodSignature;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.stereotype.Component;

@Aspect
@Component
@RequiredArgsConstructor
public class PermissionAspect {
    
    private final PermissionChecker permissionChecker;
    
    @Around("@annotation(requirePermission)")
    public Object checkPermission(ProceedingJoinPoint joinPoint, 
                                   RequirePermission requirePermission) throws Throwable {
        User user = getCurrentUser();
        String clusterId = extractClusterId(joinPoint);
        String resourceName = extractResourceName(joinPoint);
        
        permissionChecker.checkPermission(
            user,
            clusterId,
            requirePermission.resource(),
            requirePermission.action(),
            resourceName
        );
        
        return joinPoint.proceed();
    }
    
    private User getCurrentUser() {
        return (User) SecurityContextHolder.getContext()
            .getAuthentication()
            .getPrincipal();
    }
    
    private String extractClusterId(ProceedingJoinPoint joinPoint) {
        // Extract clusterId from method parameters
        MethodSignature signature = (MethodSignature) joinPoint.getSignature();
        String[] paramNames = signature.getParameterNames();
        Object[] args = joinPoint.getArgs();
        
        for (int i = 0; i < paramNames.length; i++) {
            if ("clusterId".equals(paramNames[i])) {
                return (String) args[i];
            }
        }
        return null;
    }
    
    private String extractResourceName(ProceedingJoinPoint joinPoint) {
        // Extract resource name from method parameters (topicName, groupName, etc.)
        MethodSignature signature = (MethodSignature) joinPoint.getSignature();
        String[] paramNames = signature.getParameterNames();
        Object[] args = joinPoint.getArgs();
        
        for (int i = 0; i < paramNames.length; i++) {
            if (paramNames[i].endsWith("Name") || paramNames[i].endsWith("Id")) {
                return (String) args[i];
            }
        }
        return null;
    }
}
```

### 4.5 Controller Example

```java
package com.company.kafkamanagement.presentation.rest;

import com.company.kafkamanagement.application.topic.TopicService;
import com.company.kafkamanagement.application.topic.dto.*;
import com.company.kafkamanagement.common.security.RequirePermission;
import com.company.kafkamanagement.domain.user.model.Action;
import com.company.kafkamanagement.domain.user.model.Resource;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/api/v1/clusters/{clusterId}/topics")
@RequiredArgsConstructor
@Tag(name = "Topics", description = "Topic management APIs")
public class TopicController {
    
    private final TopicService topicService;
    
    @GetMapping
    @RequirePermission(resource = Resource.TOPIC, action = Action.READ)
    @Operation(summary = "List all topics")
    public ResponseEntity<Page<TopicDTO>> listTopics(
            @PathVariable String clusterId,
            @RequestParam(required = false) String search,
            @RequestParam(defaultValue = "HIDE_INTERNAL") TopicListView view,
            Pageable pageable) {
        
        return ResponseEntity.ok(topicService.listTopics(clusterId, pageable, search, view));
    }
    
    @GetMapping("/{topicName}")
    @RequirePermission(resource = Resource.TOPIC, action = Action.READ)
    @Operation(summary = "Get topic details")
    public ResponseEntity<TopicDetailDTO> getTopic(
            @PathVariable String clusterId,
            @PathVariable String topicName) {
        
        return ResponseEntity.ok(topicService.getTopic(clusterId, topicName));
    }
    
    @PostMapping
    @RequirePermission(resource = Resource.TOPIC, action = Action.CREATE)
    @Operation(summary = "Create new topic")
    public ResponseEntity<TopicDTO> createTopic(
            @PathVariable String clusterId,
            @Valid @RequestBody TopicCreateRequest request) {
        
        return ResponseEntity.status(HttpStatus.CREATED)
            .body(topicService.createTopic(clusterId, request));
    }
    
    @DeleteMapping("/{topicName}")
    @RequirePermission(resource = Resource.TOPIC, action = Action.DELETE)
    @Operation(summary = "Delete topic")
    public ResponseEntity<Void> deleteTopic(
            @PathVariable String clusterId,
            @PathVariable String topicName) {
        
        topicService.deleteTopic(clusterId, topicName);
        return ResponseEntity.noContent().build();
    }
    
    @PostMapping("/{topicName}/partitions")
    @RequirePermission(resource = Resource.TOPIC, action = Action.UPDATE)
    @Operation(summary = "Increase topic partitions")
    public ResponseEntity<Void> increasePartitions(
            @PathVariable String clusterId,
            @PathVariable String topicName,
            @RequestParam int partitions) {
        
        topicService.increasePartitions(clusterId, topicName, partitions);
        return ResponseEntity.accepted().build();
    }
    
    @GetMapping("/{topicName}/configs")
    @RequirePermission(resource = Resource.TOPIC, action = Action.READ)
    @Operation(summary = "Get topic configurations")
    public ResponseEntity<List<TopicConfigDTO>> getTopicConfigs(
            @PathVariable String clusterId,
            @PathVariable String topicName) {
        
        return ResponseEntity.ok(topicService.getTopicConfigs(clusterId, topicName));
    }
    
    @PutMapping("/{topicName}/configs")
    @RequirePermission(resource = Resource.TOPIC, action = Action.UPDATE)
    @Operation(summary = "Update topic configurations")
    public ResponseEntity<Void> updateTopicConfigs(
            @PathVariable String clusterId,
            @PathVariable String topicName,
            @RequestBody Map<String, String> configs) {
        
        topicService.updateTopicConfigs(clusterId, topicName, configs);
        return ResponseEntity.ok().build();
    }
}
```



---

## 5. Frontend Implementation Examples

### 5.1 API Client Setup

```typescript
// src/api/client.ts
import axios, { AxiosError, AxiosInstance, InternalAxiosRequestConfig } from 'axios';
import { useAuthStore } from '@/stores/authStore';

const API_BASE_URL = import.meta.env.VITE_API_BASE_URL || '/api/v1';

export const apiClient: AxiosInstance = axios.create({
  baseURL: API_BASE_URL,
  timeout: 30000,
  headers: {
    'Content-Type': 'application/json',
  },
});

// Request interceptor - add auth token
apiClient.interceptors.request.use(
  (config: InternalAxiosRequestConfig) => {
    const token = useAuthStore.getState().accessToken;
    if (token) {
      config.headers.Authorization = `Bearer ${token}`;
    }
    return config;
  },
  (error) => Promise.reject(error)
);

// Response interceptor - handle errors
apiClient.interceptors.response.use(
  (response) => response,
  async (error: AxiosError) => {
    const originalRequest = error.config;
    
    // Handle 401 - try refresh token
    if (error.response?.status === 401 && originalRequest) {
      try {
        const newToken = await useAuthStore.getState().refreshToken();
        if (newToken && originalRequest.headers) {
          originalRequest.headers.Authorization = `Bearer ${newToken}`;
          return apiClient(originalRequest);
        }
      } catch (refreshError) {
        useAuthStore.getState().logout();
        window.location.href = '/login';
      }
    }
    
    return Promise.reject(error);
  }
);
```

### 5.2 Topic API

```typescript
// src/api/endpoints/topic.api.ts
import { apiClient } from '../client';
import type { 
  Topic, 
  TopicDetail, 
  TopicCreateRequest, 
  TopicConfig,
  PageResponse 
} from '../types/topic.types';

export const topicApi = {
  // List topics
  list: async (
    clusterId: string, 
    params?: { 
      search?: string; 
      view?: 'ALL' | 'HIDE_INTERNAL' | 'HIDE_STREAM'; 
      page?: number; 
      size?: number;
    }
  ): Promise<PageResponse<Topic>> => {
    const response = await apiClient.get(`/clusters/${clusterId}/topics`, { params });
    return response.data;
  },

  // Get topic detail
  get: async (clusterId: string, topicName: string): Promise<TopicDetail> => {
    const response = await apiClient.get(`/clusters/${clusterId}/topics/${topicName}`);
    return response.data;
  },

  // Create topic
  create: async (clusterId: string, data: TopicCreateRequest): Promise<Topic> => {
    const response = await apiClient.post(`/clusters/${clusterId}/topics`, data);
    return response.data;
  },

  // Delete topic
  delete: async (clusterId: string, topicName: string): Promise<void> => {
    await apiClient.delete(`/clusters/${clusterId}/topics/${topicName}`);
  },

  // Increase partitions
  increasePartitions: async (
    clusterId: string, 
    topicName: string, 
    partitions: number
  ): Promise<void> => {
    await apiClient.post(
      `/clusters/${clusterId}/topics/${topicName}/partitions`,
      null,
      { params: { partitions } }
    );
  },

  // Get configs
  getConfigs: async (clusterId: string, topicName: string): Promise<TopicConfig[]> => {
    const response = await apiClient.get(`/clusters/${clusterId}/topics/${topicName}/configs`);
    return response.data;
  },

  // Update configs
  updateConfigs: async (
    clusterId: string, 
    topicName: string, 
    configs: Record<string, string>
  ): Promise<void> => {
    await apiClient.put(`/clusters/${clusterId}/topics/${topicName}/configs`, configs);
  },
};
```

### 5.3 React Query Hooks

```typescript
// src/hooks/queries/useTopics.ts
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query';
import { topicApi } from '@/api/endpoints/topic.api';
import type { TopicCreateRequest } from '@/api/types/topic.types';
import { message } from 'antd';

// Query keys
export const topicKeys = {
  all: ['topics'] as const,
  lists: () => [...topicKeys.all, 'list'] as const,
  list: (clusterId: string, filters?: object) => 
    [...topicKeys.lists(), clusterId, filters] as const,
  details: () => [...topicKeys.all, 'detail'] as const,
  detail: (clusterId: string, topicName: string) => 
    [...topicKeys.details(), clusterId, topicName] as const,
  configs: (clusterId: string, topicName: string) => 
    [...topicKeys.detail(clusterId, topicName), 'configs'] as const,
};

// List topics
export function useTopics(
  clusterId: string, 
  params?: { search?: string; view?: string; page?: number; size?: number }
) {
  return useQuery({
    queryKey: topicKeys.list(clusterId, params),
    queryFn: () => topicApi.list(clusterId, params),
    enabled: !!clusterId,
    staleTime: 30 * 1000, // 30 seconds
  });
}

// Get topic detail
export function useTopic(clusterId: string, topicName: string) {
  return useQuery({
    queryKey: topicKeys.detail(clusterId, topicName),
    queryFn: () => topicApi.get(clusterId, topicName),
    enabled: !!clusterId && !!topicName,
  });
}

// Get topic configs
export function useTopicConfigs(clusterId: string, topicName: string) {
  return useQuery({
    queryKey: topicKeys.configs(clusterId, topicName),
    queryFn: () => topicApi.getConfigs(clusterId, topicName),
    enabled: !!clusterId && !!topicName,
  });
}

// Create topic mutation
export function useCreateTopic(clusterId: string) {
  const queryClient = useQueryClient();
  
  return useMutation({
    mutationFn: (data: TopicCreateRequest) => topicApi.create(clusterId, data),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: topicKeys.lists() });
      message.success('Topic created successfully');
    },
    onError: (error: Error) => {
      message.error(`Failed to create topic: ${error.message}`);
    },
  });
}

// Delete topic mutation
export function useDeleteTopic(clusterId: string) {
  const queryClient = useQueryClient();
  
  return useMutation({
    mutationFn: (topicName: string) => topicApi.delete(clusterId, topicName),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: topicKeys.lists() });
      message.success('Topic deleted successfully');
    },
    onError: (error: Error) => {
      message.error(`Failed to delete topic: ${error.message}`);
    },
  });
}

// Update configs mutation
export function useUpdateTopicConfigs(clusterId: string, topicName: string) {
  const queryClient = useQueryClient();
  
  return useMutation({
    mutationFn: (configs: Record<string, string>) => 
      topicApi.updateConfigs(clusterId, topicName, configs),
    onSuccess: () => {
      queryClient.invalidateQueries({ 
        queryKey: topicKeys.configs(clusterId, topicName) 
      });
      message.success('Configs updated successfully');
    },
    onError: (error: Error) => {
      message.error(`Failed to update configs: ${error.message}`);
    },
  });
}
```

### 5.4 Permission Hook

```typescript
// src/hooks/usePermission.ts
import { useAuthStore } from '@/stores/authStore';

export type Resource = 
  | 'cluster' 
  | 'topic' 
  | 'topic_data' 
  | 'consumer_group' 
  | 'schema' 
  | 'user' 
  | 'role';

export type Action = 'read' | 'create' | 'update' | 'delete' | 'manage';

interface Permission {
  resource: Resource;
  action: Action;
  resourcePattern?: string;
  clusterIds?: string[];
}

export function usePermission() {
  const { user } = useAuthStore();
  
  const hasPermission = (
    clusterId: string,
    resource: Resource,
    action: Action,
    resourceName?: string
  ): boolean => {
    if (!user) return false;
    
    // Admin has all permissions
    if (user.isAdmin) return true;
    
    // Check user's permissions
    return user.permissions.some((permission: Permission) => {
      // Check resource type
      if (permission.resource !== resource) return false;
      
      // Check action (manage includes all)
      if (permission.action !== 'manage' && permission.action !== action) return false;
      
      // Check cluster
      if (permission.clusterIds?.length && !permission.clusterIds.includes(clusterId)) {
        return false;
      }
      
      // Check resource pattern
      if (permission.resourcePattern && resourceName) {
        const regex = new RegExp(
          permission.resourcePattern.replace(/\*/g, '.*').replace(/\?/g, '.')
        );
        if (!regex.test(resourceName)) return false;
      }
      
      return true;
    });
  };
  
  const can = (clusterId: string, resource: Resource, action: Action, resourceName?: string) => 
    hasPermission(clusterId, resource, action, resourceName);
  
  const canRead = (clusterId: string, resource: Resource, resourceName?: string) => 
    can(clusterId, resource, 'read', resourceName);
  
  const canCreate = (clusterId: string, resource: Resource) => 
    can(clusterId, resource, 'create');
  
  const canUpdate = (clusterId: string, resource: Resource, resourceName?: string) => 
    can(clusterId, resource, 'update', resourceName);
  
  const canDelete = (clusterId: string, resource: Resource, resourceName?: string) => 
    can(clusterId, resource, 'delete', resourceName);
  
  return {
    hasPermission,
    can,
    canRead,
    canCreate,
    canUpdate,
    canDelete,
  };
}
```

### 5.5 Mermaid Diagram Component

```typescript
// src/components/mermaid/MermaidDiagram.tsx
import { useEffect, useRef, useState } from 'react';
import mermaid from 'mermaid';
import { Spin, Alert } from 'antd';

interface MermaidDiagramProps {
  chart: string;
  className?: string;
}

// Initialize mermaid
mermaid.initialize({
  startOnLoad: false,
  theme: 'default',
  securityLevel: 'loose',
  flowchart: {
    useMaxWidth: true,
    htmlLabels: true,
    curve: 'basis',
  },
});

export function MermaidDiagram({ chart, className }: MermaidDiagramProps) {
  const containerRef = useRef<HTMLDivElement>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  
  useEffect(() => {
    const renderDiagram = async () => {
      if (!containerRef.current || !chart) return;
      
      setLoading(true);
      setError(null);
      
      try {
        const id = `mermaid-${Date.now()}`;
        const { svg } = await mermaid.render(id, chart);
        containerRef.current.innerHTML = svg;
      } catch (err) {
        setError(err instanceof Error ? err.message : 'Failed to render diagram');
      } finally {
        setLoading(false);
      }
    };
    
    renderDiagram();
  }, [chart]);
  
  if (error) {
    return <Alert type="error" message="Diagram Error" description={error} />;
  }
  
  return (
    <Spin spinning={loading}>
      <div ref={containerRef} className={className} />
    </Spin>
  );
}
```

### 5.6 Topic Flow Diagram

```typescript
// src/components/mermaid/TopicFlowDiagram.tsx
import { useMemo } from 'react';
import { MermaidDiagram } from './MermaidDiagram';
import type { TopicDetail } from '@/api/types/topic.types';
import type { ConsumerGroup } from '@/api/types/consumerGroup.types';

interface TopicFlowDiagramProps {
  topic: TopicDetail;
  consumerGroups: ConsumerGroup[];
}

export function TopicFlowDiagram({ topic, consumerGroups }: TopicFlowDiagramProps) {
  const chart = useMemo(() => {
    const lines: string[] = ['flowchart LR'];
    
    // Topic node
    const topicId = `T_${topic.name.replace(/[^a-zA-Z0-9]/g, '_')}`;
    lines.push(`    ${topicId}[("📦 ${topic.name}<br/>Partitions: ${topic.partitions.length}")]`);
    
    // Style topic node
    lines.push(`    style ${topicId} fill:#1890ff,color:#fff`);
    
    // Consumer groups
    consumerGroups.forEach((group, index) => {
      const groupId = `CG_${index}`;
      const lag = group.offsets.reduce((sum, o) => sum + o.lag, 0);
      const state = group.state === 'STABLE' ? '🟢' : group.state === 'EMPTY' ? '⚪' : '🟡';
      
      lines.push(`    ${topicId} --> ${groupId}["${state} ${group.id}<br/>Lag: ${lag}"]`);
      
      // Style based on state
      if (group.state === 'STABLE') {
        lines.push(`    style ${groupId} fill:#52c41a,color:#fff`);
      } else if (group.state === 'EMPTY') {
        lines.push(`    style ${groupId} fill:#d9d9d9,color:#000`);
      } else {
        lines.push(`    style ${groupId} fill:#faad14,color:#000`);
      }
    });
    
    // If no consumer groups
    if (consumerGroups.length === 0) {
      lines.push(`    ${topicId} --> NC["No consumers"]`);
      lines.push(`    style NC fill:#ff4d4f,color:#fff`);
    }
    
    return lines.join('\n');
  }, [topic, consumerGroups]);
  
  return <MermaidDiagram chart={chart} />;
}
```

### 5.7 Cluster Topology Diagram

```typescript
// src/components/mermaid/ClusterTopologyDiagram.tsx
import { useMemo } from 'react';
import { MermaidDiagram } from './MermaidDiagram';
import type { Cluster, Node } from '@/api/types/cluster.types';

interface ClusterTopologyDiagramProps {
  cluster: Cluster;
}

export function ClusterTopologyDiagram({ cluster }: ClusterTopologyDiagramProps) {
  const chart = useMemo(() => {
    const lines: string[] = ['flowchart TB'];
    
    // Subgraph for cluster
    lines.push(`    subgraph ${cluster.id}["🏢 Kafka Cluster: ${cluster.id}"]`);
    
    // Controller node
    const controllerId = cluster.controller?.id;
    
    // Broker nodes
    cluster.nodes.forEach((node: Node) => {
      const nodeId = `N_${node.id}`;
      const isController = node.id === controllerId;
      const icon = isController ? '👑' : '🖥️';
      const label = `${icon} Broker ${node.id}<br/>${node.host}:${node.port}`;
      
      lines.push(`        ${nodeId}["${label}"]`);
      
      if (isController) {
        lines.push(`        style ${nodeId} fill:#722ed1,color:#fff`);
      } else {
        lines.push(`        style ${nodeId} fill:#1890ff,color:#fff`);
      }
    });
    
    lines.push('    end');
    
    // External connections
    lines.push(`    Client["🖥️ Clients"] --> ${cluster.id}`);
    
    if (cluster.schemaRegistry) {
      lines.push(`    SR["📋 Schema Registry"] <--> ${cluster.id}`);
      lines.push(`    style SR fill:#13c2c2,color:#fff`);
    }
    
    if (cluster.connects?.length) {
      cluster.connects.forEach((connect, i) => {
        lines.push(`    KC${i}["🔌 ${connect}"] <--> ${cluster.id}`);
        lines.push(`    style KC${i} fill:#eb2f96,color:#fff`);
      });
    }
    
    return lines.join('\n');
  }, [cluster]);
  
  return <MermaidDiagram chart={chart} />;
}
```

### 5.8 Topic List Page

```typescript
// src/pages/Topic/TopicListPage.tsx
// ... (giữ nguyên code cũ)
```

---

### 5.9 Role & Permission Management UI (Dynamic RBAC)

> **Quan trọng**: Đây là phần UI để quản lý phân quyền **động qua Web**, lưu vào Database

#### Các trang quản lý phân quyền:

| Trang | Chức năng |
|-------|-----------|
| `/settings/roles` | Danh sách roles, tạo/sửa/xóa role |
| `/settings/roles/:id` | Chi tiết role, gán permissions |
| `/settings/permissions` | Danh sách permissions, tạo/sửa/xóa |
| `/settings/users/:id` | Chi tiết user, gán roles |

#### 5.9.1 Role Management Flow

```
┌─────────────────────────────────────────────────────────────────────────┐
│                        Role Management UI Flow                           │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                          │
│  1. Admin vào trang Roles (/settings/roles)                             │
│     └── Xem danh sách roles từ Database                                 │
│                                                                          │
│  2. Click "Create Role"                                                  │
│     └── Modal form: name, description                                   │
│     └── Chọn permissions từ danh sách (Transfer component)              │
│     └── Save → POST /api/v1/roles → Lưu vào DB                         │
│                                                                          │
│  3. Click vào role để xem chi tiết                                      │
│     └── Xem danh sách permissions của role                              │
│     └── Thêm/Xóa permissions                                            │
│                                                                          │
│  4. Gán role cho user                                                    │
│     └── Vào trang User Detail                                           │
│     └── Chọn roles từ danh sách (Transfer component)                    │
│     └── Save → PUT /api/v1/users/{id}/roles                            │
│                                                                          │
└─────────────────────────────────────────────────────────────────────────┘
```

#### 5.9.2 API Hooks cho Role/Permission

```typescript
// src/hooks/queries/useRoles.ts
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query';
import { roleApi } from '@/api/endpoints/role.api';

export const roleKeys = {
  all: ['roles'] as const,
  list: (params?: object) => [...roleKeys.all, 'list', params] as const,
  detail: (id: number) => [...roleKeys.all, 'detail', id] as const,
};

export function useRoles(params?: { search?: string }) {
  return useQuery({
    queryKey: roleKeys.list(params),
    queryFn: () => roleApi.list(params),
  });
}

export function useRole(roleId: number) {
  return useQuery({
    queryKey: roleKeys.detail(roleId),
    queryFn: () => roleApi.get(roleId),
    enabled: !!roleId,
  });
}

export function useCreateRole() {
  const queryClient = useQueryClient();
  return useMutation({
    mutationFn: roleApi.create,
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: roleKeys.all });
    },
  });
}

export function useUpdateRole() {
  const queryClient = useQueryClient();
  return useMutation({
    mutationFn: ({ roleId, data }: { roleId: number; data: any }) => 
      roleApi.update(roleId, data),
    onSuccess: (_, { roleId }) => {
      queryClient.invalidateQueries({ queryKey: roleKeys.detail(roleId) });
      queryClient.invalidateQueries({ queryKey: roleKeys.list() });
    },
  });
}

export function useDeleteRole() {
  const queryClient = useQueryClient();
  return useMutation({
    mutationFn: roleApi.delete,
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: roleKeys.all });
    },
  });
}

export function useAssignPermissions() {
  const queryClient = useQueryClient();
  return useMutation({
    mutationFn: ({ roleId, permissionIds }: { roleId: number; permissionIds: number[] }) =>
      roleApi.assignPermissions(roleId, permissionIds),
    onSuccess: (_, { roleId }) => {
      queryClient.invalidateQueries({ queryKey: roleKeys.detail(roleId) });
    },
  });
}
```

#### 5.9.3 Role API Endpoints

```typescript
// src/api/endpoints/role.api.ts
import { apiClient } from '../client';
import type { Role, RoleDetail, Permission, PageResponse } from '../types/user.types';

export const roleApi = {
  // List all roles
  list: async (params?: { search?: string; page?: number; size?: number }): Promise<PageResponse<Role>> => {
    const response = await apiClient.get('/roles', { params });
    return response.data;
  },

  // Get role detail with permissions
  get: async (roleId: number): Promise<RoleDetail> => {
    const response = await apiClient.get(`/roles/${roleId}`);
    return response.data;
  },

  // Create new role
  create: async (data: { name: string; description?: string; permissionIds?: number[] }): Promise<Role> => {
    const response = await apiClient.post('/roles', data);
    return response.data;
  },

  // Update role
  update: async (roleId: number, data: { name: string; description?: string }): Promise<Role> => {
    const response = await apiClient.put(`/roles/${roleId}`, data);
    return response.data;
  },

  // Delete role
  delete: async (roleId: number): Promise<void> => {
    await apiClient.delete(`/roles/${roleId}`);
  },

  // Assign permissions to role
  assignPermissions: async (roleId: number, permissionIds: number[]): Promise<RoleDetail> => {
    const response = await apiClient.put(`/roles/${roleId}/permissions`, permissionIds);
    return response.data;
  },

  // Add single permission
  addPermission: async (roleId: number, permissionId: number): Promise<RoleDetail> => {
    const response = await apiClient.post(`/roles/${roleId}/permissions/${permissionId}`);
    return response.data;
  },

  // Remove single permission
  removePermission: async (roleId: number, permissionId: number): Promise<RoleDetail> => {
    const response = await apiClient.delete(`/roles/${roleId}/permissions/${permissionId}`);
    return response.data;
  },
};

// Permission API
export const permissionApi = {
  list: async (params?: { resource?: string }): Promise<PageResponse<Permission>> => {
    const response = await apiClient.get('/permissions', { params });
    return response.data;
  },

  getResourceTypes: async () => {
    const response = await apiClient.get('/permissions/resource-types');
    return response.data;
  },

  getActionTypes: async () => {
    const response = await apiClient.get('/permissions/action-types');
    return response.data;
  },

  create: async (data: {
    resource: string;
    action: string;
    resourcePattern?: string;
    clusterIds?: string[];
  }): Promise<Permission> => {
    const response = await apiClient.post('/permissions', data);
    return response.data;
  },

  update: async (permissionId: number, data: any): Promise<Permission> => {
    const response = await apiClient.put(`/permissions/${permissionId}`, data);
    return response.data;
  },

  delete: async (permissionId: number): Promise<void> => {
    await apiClient.delete(`/permissions/${permissionId}`);
  },
};

// User role assignment
export const userApi = {
  // ... other methods
  
  assignRoles: async (userId: number, roleIds: number[]): Promise<any> => {
    const response = await apiClient.put(`/users/${userId}/roles`, roleIds);
    return response.data;
  },

  getUserPermissions: async (userId: number): Promise<Permission[]> => {
    const response = await apiClient.get(`/users/${userId}/permissions`);
    return response.data;
  },
};
```



---

## 6. Database Schema

### 6.1 Flyway Migrations

```sql
-- V1__create_users_table.sql
CREATE TABLE users (
    id BIGSERIAL PRIMARY KEY,
    username VARCHAR(50) NOT NULL UNIQUE,
    email VARCHAR(100) NOT NULL UNIQUE,
    password_hash VARCHAR(255) NOT NULL,
    full_name VARCHAR(100),
    is_admin BOOLEAN DEFAULT FALSE,
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    last_login_at TIMESTAMP
);

CREATE INDEX idx_users_username ON users(username);
CREATE INDEX idx_users_email ON users(email);

-- V2__create_roles_table.sql
CREATE TABLE roles (
    id BIGSERIAL PRIMARY KEY,
    name VARCHAR(50) NOT NULL UNIQUE,
    description VARCHAR(255),
    is_system BOOLEAN DEFAULT FALSE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE user_roles (
    user_id BIGINT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    role_id BIGINT NOT NULL REFERENCES roles(id) ON DELETE CASCADE,
    PRIMARY KEY (user_id, role_id)
);

-- V3__create_permissions_table.sql
CREATE TABLE permissions (
    id BIGSERIAL PRIMARY KEY,
    resource VARCHAR(50) NOT NULL,
    action VARCHAR(50) NOT NULL,
    resource_pattern VARCHAR(255),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE permission_clusters (
    permission_id BIGINT NOT NULL REFERENCES permissions(id) ON DELETE CASCADE,
    cluster_id VARCHAR(100) NOT NULL,
    PRIMARY KEY (permission_id, cluster_id)
);

CREATE TABLE role_permissions (
    role_id BIGINT NOT NULL REFERENCES roles(id) ON DELETE CASCADE,
    permission_id BIGINT NOT NULL REFERENCES permissions(id) ON DELETE CASCADE,
    PRIMARY KEY (role_id, permission_id)
);

-- V4__create_cluster_configs_table.sql
CREATE TABLE cluster_configs (
    id BIGSERIAL PRIMARY KEY,
    cluster_id VARCHAR(100) NOT NULL UNIQUE,
    name VARCHAR(100) NOT NULL,
    bootstrap_servers VARCHAR(500) NOT NULL,
    security_protocol VARCHAR(50),
    sasl_mechanism VARCHAR(50),
    sasl_jaas_config TEXT,
    ssl_truststore_location VARCHAR(255),
    ssl_truststore_password VARCHAR(255),
    ssl_keystore_location VARCHAR(255),
    ssl_keystore_password VARCHAR(255),
    ssl_key_password VARCHAR(255),
    schema_registry_url VARCHAR(255),
    schema_registry_username VARCHAR(100),
    schema_registry_password VARCHAR(255),
    additional_properties JSONB,
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_cluster_configs_cluster_id ON cluster_configs(cluster_id);

-- V5__create_audit_logs_table.sql
CREATE TABLE audit_logs (
    id BIGSERIAL PRIMARY KEY,
    user_id BIGINT REFERENCES users(id),
    username VARCHAR(50),
    cluster_id VARCHAR(100),
    resource_type VARCHAR(50) NOT NULL,
    resource_name VARCHAR(255),
    action VARCHAR(50) NOT NULL,
    details JSONB,
    ip_address VARCHAR(45),
    user_agent VARCHAR(255),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_audit_logs_user_id ON audit_logs(user_id);
CREATE INDEX idx_audit_logs_cluster_id ON audit_logs(cluster_id);
CREATE INDEX idx_audit_logs_created_at ON audit_logs(created_at);
CREATE INDEX idx_audit_logs_resource ON audit_logs(resource_type, resource_name);

-- V6__seed_default_data.sql
-- Default roles
INSERT INTO roles (name, description, is_system) VALUES
    ('admin', 'Full system administrator', TRUE),
    ('developer', 'Developer with read/write access to non-production', TRUE),
    ('viewer', 'Read-only access', TRUE);

-- Admin permissions (all resources, all actions)
INSERT INTO permissions (resource, action) VALUES
    ('cluster', 'manage'),
    ('topic', 'manage'),
    ('topic_data', 'manage'),
    ('consumer_group', 'manage'),
    ('schema', 'manage'),
    ('node', 'manage'),
    ('acl', 'manage'),
    ('user', 'manage'),
    ('role', 'manage');

-- Link admin role to all permissions
INSERT INTO role_permissions (role_id, permission_id)
SELECT 
    (SELECT id FROM roles WHERE name = 'admin'),
    id
FROM permissions;

-- Developer permissions
INSERT INTO permissions (resource, action, resource_pattern) VALUES
    ('topic', 'read', NULL),
    ('topic', 'create', 'dev-*'),
    ('topic', 'update', 'dev-*'),
    ('topic', 'delete', 'dev-*'),
    ('topic_data', 'read', NULL),
    ('topic_data', 'create', 'dev-*'),
    ('consumer_group', 'read', NULL),
    ('consumer_group', 'update', 'dev-*'),
    ('schema', 'read', NULL),
    ('schema', 'create', 'dev-*');

-- Link developer role to permissions
INSERT INTO role_permissions (role_id, permission_id)
SELECT 
    (SELECT id FROM roles WHERE name = 'developer'),
    id
FROM permissions
WHERE id > 9; -- Skip admin permissions

-- Viewer permissions
INSERT INTO permissions (resource, action) VALUES
    ('topic', 'read'),
    ('topic_data', 'read'),
    ('consumer_group', 'read'),
    ('schema', 'read'),
    ('node', 'read');

-- Default admin user (password: admin123)
INSERT INTO users (username, email, password_hash, full_name, is_admin)
VALUES ('admin', 'admin@example.com', 
    '$2a$10$N9qo8uLOickgx2ZMRZoMyeIjZRGdjGj/n3.rsS3/r/MqoYvxqQGKy', 
    'System Administrator', TRUE);

INSERT INTO user_roles (user_id, role_id)
VALUES (1, (SELECT id FROM roles WHERE name = 'admin'));
```

---

## 7. Development Phases

### Phase 1: Foundation (2-3 weeks)

**Backend:**
- [ ] Project setup với Spring Boot 3.x
- [ ] Security configuration (JWT, Spring Security)
- [ ] Database setup với Flyway migrations
- [ ] User/Role/Permission entities và repositories
- [ ] Authentication APIs (login, refresh token, logout)
- [ ] User management APIs
- [ ] Global exception handling
- [ ] API documentation với OpenAPI

**Frontend:**
- [ ] Project setup với Vite + React + TypeScript
- [ ] Routing configuration
- [ ] Auth store và API client
- [ ] Login page
- [ ] Main layout (sidebar, header)
- [ ] Permission hook

### Phase 2: Kafka Core (3-4 weeks)

**Backend:**
- [ ] KafkaClientFactory
- [ ] KafkaAdminClientWrapper
- [ ] Cluster management APIs
- [ ] Topic APIs (CRUD, configs)
- [ ] Consumer Group APIs
- [ ] Node/Broker APIs
- [ ] Caching với Redis/Caffeine

**Frontend:**
- [ ] Cluster list/detail pages
- [ ] Topic list/detail pages
- [ ] Topic create/edit forms
- [ ] Consumer group list/detail pages
- [ ] Node list page
- [ ] React Query integration

### Phase 3: Message Operations (2-3 weeks)

**Backend:**
- [ ] KafkaConsumerWrapper
- [ ] KafkaProducerWrapper
- [ ] Message consume APIs
- [ ] Message produce APIs
- [ ] Message search (SSE)
- [ ] Topic tail (SSE)

**Frontend:**
- [ ] Message browser component
- [ ] Message detail modal
- [ ] Message producer form
- [ ] Search functionality
- [ ] Real-time tail với SSE

### Phase 4: Schema Registry (1-2 weeks)

**Backend:**
- [ ] SchemaRegistryClientWrapper
- [ ] Schema APIs (CRUD, versions)
- [ ] Schema compatibility check

**Frontend:**
- [ ] Schema list/detail pages
- [ ] Schema editor (Monaco)
- [ ] Version comparison

### Phase 5: Visualization (1-2 weeks)

**Frontend:**
- [ ] Mermaid integration
- [ ] Topic flow diagram
- [ ] Consumer group diagram
- [ ] Cluster topology diagram
- [ ] Dashboard với charts

### Phase 6: Advanced Features (2-3 weeks)

**Backend:**
- [ ] Audit logging
- [ ] Kafka Connect APIs (optional)
- [ ] KsqlDB APIs (optional)
- [ ] ACL APIs

**Frontend:**
- [ ] Audit log viewer
- [ ] Connect management (optional)
- [ ] KsqlDB console (optional)
- [ ] ACL viewer

### Phase 7: Testing & Deployment (2 weeks)

- [ ] Unit tests (JUnit, Vitest)
- [ ] Integration tests (Testcontainers)
- [ ] E2E tests (Playwright)
- [ ] Docker configuration
- [ ] Kubernetes manifests
- [ ] CI/CD pipeline
- [ ] Documentation

---

## 8. Docker & Deployment

### 8.1 Backend Dockerfile

```dockerfile
# Build stage
FROM eclipse-temurin:21-jdk-alpine AS build
WORKDIR /app
COPY gradle gradle
COPY gradlew build.gradle.kts settings.gradle.kts ./
RUN ./gradlew dependencies --no-daemon
COPY src src
RUN ./gradlew bootJar --no-daemon -x test

# Runtime stage
FROM eclipse-temurin:21-jre-alpine
WORKDIR /app

# Create non-root user
RUN addgroup -S appgroup && adduser -S appuser -G appgroup
USER appuser

COPY --from=build /app/build/libs/*.jar app.jar

EXPOSE 8080
HEALTHCHECK --interval=30s --timeout=3s \
  CMD wget -q --spider http://localhost:8080/actuator/health || exit 1

ENTRYPOINT ["java", "-jar", "app.jar"]
```

### 8.2 Frontend Dockerfile

```dockerfile
# Build stage
FROM node:20-alpine AS build
WORKDIR /app
COPY package*.json ./
RUN npm ci
COPY . .
RUN npm run build

# Runtime stage
FROM nginx:alpine
COPY --from=build /app/dist /usr/share/nginx/html
COPY nginx.conf /etc/nginx/conf.d/default.conf
EXPOSE 80
CMD ["nginx", "-g", "daemon off;"]
```

### 8.3 Docker Compose

```yaml
version: '3.8'

services:
  backend:
    build:
      context: ./kafka-management-backend
      dockerfile: Dockerfile
    ports:
      - "8080:8080"
    environment:
      - SPRING_PROFILES_ACTIVE=prod
      - SPRING_DATASOURCE_URL=jdbc:postgresql://postgres:5432/kafka_management
      - SPRING_DATASOURCE_USERNAME=postgres
      - SPRING_DATASOURCE_PASSWORD=postgres
      - SPRING_DATA_REDIS_HOST=redis
      - JWT_SECRET=${JWT_SECRET}
    depends_on:
      - postgres
      - redis
    healthcheck:
      test: ["CMD", "wget", "-q", "--spider", "http://localhost:8080/actuator/health"]
      interval: 30s
      timeout: 10s
      retries: 3

  frontend:
    build:
      context: ./kafka-management-frontend
      dockerfile: Dockerfile
    ports:
      - "80:80"
    depends_on:
      - backend

  postgres:
    image: postgres:16-alpine
    environment:
      - POSTGRES_DB=kafka_management
      - POSTGRES_USER=postgres
      - POSTGRES_PASSWORD=postgres
    volumes:
      - postgres_data:/var/lib/postgresql/data
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U postgres"]
      interval: 10s
      timeout: 5s
      retries: 5

  redis:
    image: redis:7-alpine
    volumes:
      - redis_data:/data
    healthcheck:
      test: ["CMD", "redis-cli", "ping"]
      interval: 10s
      timeout: 5s
      retries: 5

volumes:
  postgres_data:
  redis_data:
```

---

## 9. Best Practices Summary

### 9.1 Backend

| Aspect | Practice |
|--------|----------|
| **Architecture** | Hexagonal/Clean Architecture với clear layer separation |
| **API Design** | RESTful, versioned (`/api/v1`), consistent naming |
| **Security** | JWT tokens, RBAC, input validation, audit logging |
| **Error Handling** | Global exception handler, meaningful error messages |
| **Caching** | Multi-level (local + Redis), cache invalidation strategy |
| **Testing** | Unit + Integration + E2E, Testcontainers for Kafka |
| **Documentation** | OpenAPI/Swagger, code comments |

### 9.2 Frontend

| Aspect | Practice |
|--------|----------|
| **State Management** | Server state (React Query) + Client state (Zustand) |
| **Type Safety** | Strict TypeScript, shared types với backend |
| **Component Design** | Atomic design, reusable components |
| **Performance** | Code splitting, lazy loading, memoization |
| **Error Handling** | Error boundaries, toast notifications |
| **Accessibility** | ARIA labels, keyboard navigation |

### 9.3 DevOps

| Aspect | Practice |
|--------|----------|
| **Containerization** | Multi-stage Docker builds, non-root users |
| **CI/CD** | Automated testing, linting, security scanning |
| **Monitoring** | Health checks, metrics (Prometheus), logging (ELK) |
| **Security** | Secrets management, network policies |

---

## 10. Estimated Timeline

| Phase | Duration | Deliverables |
|-------|----------|--------------|
| Phase 1: Foundation | 2-3 weeks | Auth, User management, Project setup |
| Phase 2: Kafka Core | 3-4 weeks | Topics, Consumer Groups, Nodes |
| Phase 3: Messages | 2-3 weeks | Produce, Consume, Search, Tail |
| Phase 4: Schema Registry | 1-2 weeks | Schema CRUD, Versions |
| Phase 5: Visualization | 1-2 weeks | Mermaid diagrams, Dashboard |
| Phase 6: Advanced | 2-3 weeks | Audit, Connect, KsqlDB |
| Phase 7: Testing & Deploy | 2 weeks | Tests, Docker, CI/CD |
| **Total** | **13-19 weeks** | **Full MVP** |

---

*Document Version: 1.0*
*Last Updated: 2026-05-17*
