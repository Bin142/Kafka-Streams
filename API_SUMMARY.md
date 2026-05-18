# Kafka Management System - API Summary

> **Last Updated**: 2026-05-18  
> **Status**: All APIs implemented according to REQUIREMENTS.md

---

## 📁 Phân Loại Files Trong Project

### 📘 Files Hướng Dẫn (Documentation/Reference)
Các file này là **tài liệu tham khảo**, mô tả yêu cầu và kế hoạch phát triển:

| File | Mô tả |
|------|-------|
| `REQUIREMENTS.md` | **Tài liệu yêu cầu chính** - Định nghĩa tất cả functional/non-functional requirements, API endpoints cần implement, database schema, acceptance criteria |
| `KAFKA_MANAGEMENT_DEVELOPMENT_PLAN.md` | **Kế hoạch phát triển** - Roadmap, timeline, tech stack decisions, architecture overview |
| `AKHQ_PROJECT_ANALYSIS.md` | **Phân tích tham khảo** - Phân tích AKHQ (open-source Kafka UI) để học hỏi features và UX patterns |
| `HELP.md` | Spring Boot reference documentation links |
| `README.md` | Hướng dẫn chung về project |

### 📗 Files Hiện Trạng (Current Implementation)
Các file này là **code thực tế** đang chạy trong project:

| Folder/File | Mô tả |
|-------------|-------|
| `API_SUMMARY.md` | **File này** - Tổng hợp API đã implement, mapping với requirements |
| `pom.xml` | Maven build configuration, dependencies |
| `docker-compose.yml` | Docker setup cho Kafka, Zookeeper, Schema Registry, PostgreSQL |
| `src/main/java/**` | **Backend source code** (Java/Spring Boot) |
| `src/main/resources/**` | Configuration files (application.yml, etc.) |
| `kafka-management-ui/**` | **Frontend source code** (React/TypeScript/Vite) |

---

## 🔄 Mapping: Requirements → Implementation

| Requirement File | Implementation Status |
|------------------|----------------------|
| `REQUIREMENTS.md` Section 2.1 (Auth) | ✅ `AuthController.java`, `AuthService.java` |
| `REQUIREMENTS.md` Section 2.2 (Clusters) | ✅ `ClusterController.java`, `ClusterService.java` |
| `REQUIREMENTS.md` Section 2.3 (Topics) | ✅ `TopicController.java`, `TopicService.java` |
| `REQUIREMENTS.md` Section 2.4 (Messages) | ✅ `MessageController.java`, `MessageTailController.java` |
| `REQUIREMENTS.md` Section 2.5 (Consumer Groups) | ✅ `ConsumerGroupController.java`, `ConsumerGroupService.java` |
| `REQUIREMENTS.md` Section 2.6 (Schema Registry) | ✅ `SchemaController.java`, `SchemaService.java` |
| `REQUIREMENTS.md` Section 2.7 (Kafka Connect) | ✅ `ConnectorController.java`, `ConnectorService.java` |
| `REQUIREMENTS.md` Section 2.8 (ACLs) | ✅ `AclController.java`, `AclService.java` |
| `REQUIREMENTS.md` Section 2.9 (Users) | ✅ `UserController.java`, `UserService.java` |
| `REQUIREMENTS.md` Section 2.10 (Roles) | ✅ `RoleController.java`, `RoleService.java` |
| `REQUIREMENTS.md` Section 2.11 (Permissions) | ✅ `PermissionController.java`, `PermissionService.java` |

---

## 📂 Backend Structure (Hiện Trạng)

```
src/main/java/com/kafkamanagement/
│
├── 📁 application/                    # Business Logic Layer
│   ├── acl/
│   │   ├── AclService.java           # ACL business logic
│   │   └── dto/
│   │       ├── AclDTO.java
│   │       ├── AclCreateRequest.java
│   │       └── AclDeleteRequest.java
│   │
│   ├── auth/
│   │   ├── AuthService.java          # Authentication logic
│   │   └── dto/
│   │       ├── LoginRequest.java
│   │       ├── LoginResponse.java
│   │       └── RefreshTokenRequest.java
│   │
│   ├── cluster/
│   │   ├── ClusterService.java       # Cluster management logic
│   │   └── dto/
│   │       ├── ClusterDTO.java
│   │       └── ClusterDetailDTO.java (includes NodeDTO, ConfigEntryDTO)
│   │
│   ├── connector/
│   │   ├── ConnectorService.java     # Kafka Connect logic
│   │   └── dto/
│   │       ├── ConnectorDTO.java
│   │       ├── ConnectorCreateRequest.java
│   │       └── ConnectorPluginDTO.java
│   │
│   ├── consumergroup/
│   │   ├── ConsumerGroupService.java # Consumer group logic
│   │   └── dto/
│   │       ├── ConsumerGroupDTO.java
│   │       ├── ConsumerGroupDetailDTO.java
│   │       └── OffsetResetRequest.java
│   │
│   ├── message/
│   │   ├── MessageService.java       # Message browse/produce logic
│   │   ├── MessageTailService.java   # SSE real-time streaming
│   │   └── dto/
│   │       ├── MessageDTO.java
│   │       ├── MessageBrowseRequest.java
│   │       ├── MessageBrowseResponse.java
│   │       ├── MessageProduceRequest.java
│   │       ├── CopyMessageRequest.java
│   │       ├── CopyMessageResponse.java
│   │       ├── ExportMessageRequest.java
│   │       └── TailMessageDTO.java
│   │
│   ├── schema/
│   │   ├── SchemaService.java        # Schema Registry logic
│   │   └── dto/
│   │
│   ├── topic/
│   │   ├── TopicService.java         # Topic management logic
│   │   └── dto/
│   │
│   └── user/
│       ├── UserService.java          # User CRUD
│       ├── RoleService.java          # Role CRUD
│       ├── PermissionService.java    # Permission CRUD
│       └── dto/
│
├── 📁 common/                         # Shared utilities
│   ├── exception/
│   │   ├── BusinessException.java
│   │   ├── ResourceNotFoundException.java
│   │   └── GlobalExceptionHandler.java
│   └── security/
│       ├── JwtTokenProvider.java
│       ├── JwtAuthenticationFilter.java
│       └── PermissionChecker.java
│
├── 📁 domain/                         # Domain entities
│   └── user/
│       ├── model/
│       │   ├── Resource.java         # Enum: CLUSTER, TOPIC, ACL, etc.
│       │   └── Action.java           # Enum: READ, CREATE, UPDATE, DELETE
│       ├── entity/
│       │   ├── UserEntity.java
│       │   ├── RoleEntity.java
│       │   ├── PermissionEntity.java
│       │   └── RefreshTokenEntity.java
│       └── repository/
│
├── 📁 infrastructure/                 # External integrations
│   └── kafka/
│       ├── KafkaAdminWrapper.java    # Kafka AdminClient wrapper
│       ├── KafkaClientFactory.java   # Create Kafka clients
│       ├── KafkaConnectClient.java   # REST client for Kafka Connect
│       ├── SchemaRegistryClientFactory.java
│       └── config/
│           ├── ClusterConfig.java
│           └── ClustersConfigLoader.java
│
└── 📁 presentation/                   # REST API Layer
    └── rest/
        ├── AclController.java         # ACL endpoints
        ├── AuthController.java        # Auth endpoints
        ├── ClusterController.java     # Cluster endpoints
        ├── ConnectorController.java   # Kafka Connect endpoints
        ├── ConsumerGroupController.java
        ├── MessageController.java     # Message browse/produce
        ├── MessageTailController.java # SSE streaming
        ├── PermissionController.java
        ├── RoleController.java
        ├── SchemaController.java
        ├── TopicController.java
        └── UserController.java
```

---

## 📂 Frontend Structure (Hiện Trạng)

```
kafka-management-ui/
│
├── 📁 src/
│   ├── 📁 api/                        # API client layer
│   │   ├── client.ts                 # Axios instance
│   │   ├── clusters.ts               # Cluster API calls
│   │   ├── topics.ts                 # Topic API calls
│   │   ├── messages.ts               # Message API calls
│   │   ├── consumerGroups.ts         # Consumer group API calls
│   │   ├── schemas.ts                # Schema Registry API calls
│   │   ├── users.ts                  # User API calls
│   │   ├── roles.ts                  # Role API calls
│   │   ├── mockData.ts               # Mock data fallback
│   │   ├── types.ts                  # TypeScript interfaces
│   │   └── index.ts
│   │
│   ├── 📁 pages/                      # Page components
│   │   ├── Dashboard.tsx             # Home dashboard
│   │   ├── Clusters.tsx              # Cluster list
│   │   ├── Topics.tsx                # Topic list
│   │   ├── TopicDetail.tsx           # Topic detail + Mermaid flow
│   │   ├── ConsumerGroups.tsx        # Consumer group list
│   │   ├── ConsumerGroupDetail.tsx   # Consumer group detail
│   │   ├── Schemas.tsx               # Schema Registry
│   │   ├── Connectors.tsx            # Kafka Connect
│   │   ├── ACLs.tsx                  # ACL management
│   │   ├── Users.tsx                 # User management
│   │   └── Roles.tsx                 # Role management
│   │
│   ├── 📁 layouts/
│   │   └── MainLayout.tsx            # App layout with sidebar
│   │
│   ├── 📁 store/
│   │   └── clusterStore.ts           # Zustand state management
│   │
│   ├── App.tsx                       # Main app with routes
│   ├── App.css
│   ├── main.tsx                      # Entry point
│   └── index.css
│
├── package.json                       # Dependencies
├── vite.config.ts                     # Vite configuration
└── tsconfig.json                      # TypeScript config
```

---

## API Endpoints (Chi Tiết)

### 1. Authentication (`AuthController`)
| Method | Endpoint | Description |
|--------|----------|-------------|
| POST | `/api/v1/auth/login` | Login with username/password |
| POST | `/api/v1/auth/refresh` | Refresh access token |
| POST | `/api/v1/auth/logout` | Logout and invalidate refresh token |

### 2. Clusters (`ClusterController`)
| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/api/v1/clusters` | List all clusters user has access to |
| GET | `/api/v1/clusters/{clusterId}` | Get cluster details |
| GET | `/api/v1/clusters/{clusterId}/nodes` | List cluster nodes |
| GET | `/api/v1/clusters/{clusterId}/nodes/{nodeId}` | Get node details |
| GET | `/api/v1/clusters/{clusterId}/nodes/{nodeId}/configs` | Get node (broker) configs |

### 3. Topics (`TopicController`)
| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/api/v1/clusters/{clusterId}/topics` | List topics with filtering |
| POST | `/api/v1/clusters/{clusterId}/topics` | Create topic |
| GET | `/api/v1/clusters/{clusterId}/topics/{topicName}` | Get topic details |
| DELETE | `/api/v1/clusters/{clusterId}/topics/{topicName}` | Delete topic |
| GET | `/api/v1/clusters/{clusterId}/topics/{topicName}/configs` | Get topic configs |
| PUT | `/api/v1/clusters/{clusterId}/topics/{topicName}/configs` | Update topic configs |
| POST | `/api/v1/clusters/{clusterId}/topics/{topicName}/partitions` | Increase partitions |
| GET | `/api/v1/clusters/{clusterId}/topics/{topicName}/consumer-groups` | Get consumer groups for topic |

### 4. Messages (`MessageController` + `MessageTailController`)
| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/api/v1/clusters/{clusterId}/topics/{topicName}/messages` | Browse messages |
| POST | `/api/v1/clusters/{clusterId}/topics/{topicName}/messages` | Produce message |
| DELETE | `/api/v1/clusters/{clusterId}/topics/{topicName}/messages` | Delete message (tombstone) |
| DELETE | `/api/v1/clusters/{clusterId}/topics/{topicName}/messages/all` | Empty topic |
| GET | `/api/v1/clusters/{clusterId}/topics/{topicName}/messages/tail` | Real-time tail (SSE) |
| GET | `/api/v1/clusters/{clusterId}/topics/{topicName}/messages/export` | Export messages (JSON/CSV) |
| POST | `/api/v1/clusters/{clusterId}/topics/{topicName}/messages/copy` | Copy messages |

### 5. Consumer Groups (`ConsumerGroupController`)
| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/api/v1/clusters/{clusterId}/consumer-groups` | List consumer groups |
| GET | `/api/v1/clusters/{clusterId}/consumer-groups/{groupId}` | Get group details |
| DELETE | `/api/v1/clusters/{clusterId}/consumer-groups/{groupId}` | Delete group |
| GET | `/api/v1/clusters/{clusterId}/consumer-groups/{groupId}/offsets` | Get group offsets |
| POST | `/api/v1/clusters/{clusterId}/consumer-groups/{groupId}/offsets/reset` | Reset offsets |
| DELETE | `/api/v1/clusters/{clusterId}/consumer-groups/{groupId}/offsets/{topicName}` | Delete offsets for topic |

### 6. Schema Registry (`SchemaController`)
| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/api/v1/clusters/{clusterId}/schemas` | List schemas (subjects) |
| POST | `/api/v1/clusters/{clusterId}/schemas` | Register schema |
| GET | `/api/v1/clusters/{clusterId}/schemas/{subject}` | Get schema |
| DELETE | `/api/v1/clusters/{clusterId}/schemas/{subject}` | Delete schema |
| GET | `/api/v1/clusters/{clusterId}/schemas/{subject}/versions` | List versions |
| GET | `/api/v1/clusters/{clusterId}/schemas/{subject}/versions/{version}` | Get specific version |
| GET | `/api/v1/clusters/{clusterId}/schemas/{subject}/config` | Get compatibility config |
| PUT | `/api/v1/clusters/{clusterId}/schemas/{subject}/config` | Update compatibility config |

### 7. Kafka Connect (`ConnectorController`)
| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/api/v1/clusters/{clusterId}/connectors` | List connectors |
| POST | `/api/v1/clusters/{clusterId}/connectors` | Create connector |
| GET | `/api/v1/clusters/{clusterId}/connectors/{name}` | Get connector |
| PUT | `/api/v1/clusters/{clusterId}/connectors/{name}/config` | Update config |
| DELETE | `/api/v1/clusters/{clusterId}/connectors/{name}` | Delete connector |
| POST | `/api/v1/clusters/{clusterId}/connectors/{name}/restart` | Restart connector |
| PUT | `/api/v1/clusters/{clusterId}/connectors/{name}/pause` | Pause connector |
| PUT | `/api/v1/clusters/{clusterId}/connectors/{name}/resume` | Resume connector |
| POST | `/api/v1/clusters/{clusterId}/connectors/{name}/tasks/{taskId}/restart` | Restart task |
| GET | `/api/v1/clusters/{clusterId}/connectors/plugins` | List plugins |
| PUT | `/api/v1/clusters/{clusterId}/connectors/plugins/{pluginName}/config/validate` | Validate config |

### 8. ACLs (`AclController`)
| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/api/v1/clusters/{clusterId}/acls` | List ACLs |
| POST | `/api/v1/clusters/{clusterId}/acls` | Create ACL |
| DELETE | `/api/v1/clusters/{clusterId}/acls` | Delete ACLs |

### 9. Users (`UserController`)
| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/api/v1/users` | List users |
| POST | `/api/v1/users` | Create user |
| GET | `/api/v1/users/{id}` | Get user |
| PUT | `/api/v1/users/{id}` | Update user |
| DELETE | `/api/v1/users/{id}` | Delete user |
| PUT | `/api/v1/users/{id}/roles` | Assign roles |

### 10. Roles (`RoleController`)
| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/api/v1/roles` | List roles |
| POST | `/api/v1/roles` | Create role |
| GET | `/api/v1/roles/{id}` | Get role |
| PUT | `/api/v1/roles/{id}` | Update role |
| DELETE | `/api/v1/roles/{id}` | Delete role |
| PUT | `/api/v1/roles/{id}/permissions` | Assign permissions |

### 11. Permissions (`PermissionController`)
| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/api/v1/permissions` | List permissions |
| POST | `/api/v1/permissions` | Create permission |
| PUT | `/api/v1/permissions/{id}` | Update permission |
| DELETE | `/api/v1/permissions/{id}` | Delete permission |

---

## 🚀 Cách Chạy Project

### Backend
```bash
# Development mode (H2 database, auth disabled)
./mvnw spring-boot:run -Dspring-boot.run.profiles=dev

# Production mode
./mvnw spring-boot:run
```

### Frontend
```bash
cd kafka-management-ui
npm install
npm run dev
```

### Docker (Kafka + Dependencies)
```bash
docker-compose up -d
```

---

## 📝 Notes

- **Context path**: `/api` → Full URL: `http://localhost:8080/api/v1/...`
- **Dev mode**: Authentication disabled (`security.auth.disabled: true`)
- **SSE endpoint**: Uses `text/event-stream` content type
- **Export endpoint**: Returns file download (JSON/CSV)
- **Mock data**: Frontend falls back to mock data when Kafka disconnected
