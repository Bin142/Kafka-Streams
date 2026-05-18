# Kafka Management System - Requirements Document

> **Version**: 1.0  
> **Created**: 2026-05-17  
> **Tech Stack**: Java 21 + Spring Boot 3.x + React 18 + TypeScript + Ant Design

---

## 1. Tổng Quan Dự Án

### 1.1 Mục Tiêu
Xây dựng hệ thống quản lý Kafka với giao diện web custom, hỗ trợ:
- Quản lý multi-cluster Kafka
- Phân quyền RBAC động qua Database + Web UI
- Visualize topology bằng Mermaid diagrams
- Tích hợp Schema Registry và Kafka Connect

### 1.2 Đối Tượng Sử Dụng
- **Admin**: Quản lý toàn bộ hệ thống, users, roles, permissions
- **Developer**: Quản lý topics, browse messages, consumer groups
- **Viewer**: Chỉ xem thông tin, không có quyền thay đổi

### 1.3 Quy Mô
- **Concurrent users**: ~50 users
- **Multi-tenancy**: User chỉ thấy clusters được phân quyền

---

## 2. Functional Requirements

### 2.1 Authentication & Authorization

#### FR-AUTH-01: JWT Authentication
- Đăng nhập bằng username/password
- Trả về JWT access token (expire 1h) và refresh token (expire 7d)
- Refresh token để lấy access token mới

#### FR-AUTH-02: Dynamic RBAC
- Quản lý Users, Roles, Permissions qua Web UI
- Permissions lưu trong Database (PostgreSQL)
- Permission structure:
  - `resource`: CLUSTER, TOPIC, TOPIC_DATA, CONSUMER_GROUP, SCHEMA, CONNECT, ACL, USER, ROLE
  - `action`: READ, CREATE, UPDATE, DELETE, MANAGE
  - `resource_pattern`: Glob pattern (e.g., `dev-*`, `prod-orders-*`)
  - `cluster_ids`: Danh sách cluster IDs được phép

#### FR-AUTH-03: Multi-tenancy
- User chỉ thấy clusters được phân quyền
- Filter data theo permissions của user

---

### 2.2 Cluster Management

#### FR-CLUSTER-01: List Clusters
- Hiển thị danh sách clusters user có quyền truy cập
- Thông tin: name, bootstrap servers, status, node count

#### FR-CLUSTER-02: Cluster Details
- Cluster ID, controller node
- Danh sách nodes (brokers) với host:port
- Cluster configs

#### FR-CLUSTER-03: Node Management
- List nodes trong cluster
- Node details: id, host, port, rack
- Node configs (broker configs)
- Log directories info

#### FR-CLUSTER-04: Cluster Topology Diagram
- Mermaid diagram hiển thị topology cluster
- Nodes, controller, Schema Registry, Kafka Connect

---

### 2.3 Topic Management

#### FR-TOPIC-01: List Topics
- Danh sách topics với filter:
  - ALL: Tất cả topics
  - HIDE_INTERNAL: Ẩn internal topics (`_*`, `__*`)
  - HIDE_STREAM: Ẩn stream topics (`*-changelog`, `*-repartition`)
- Search by name
- Pagination

#### FR-TOPIC-02: Topic Details
- Tên, partition count, replication factor
- Danh sách partitions với leader, replicas, ISR
- First/Last offsets của mỗi partition
- Total messages count

#### FR-TOPIC-03: Create Topic
- Input: name, partitions, replication factor
- Optional: topic configs (retention.ms, cleanup.policy, etc.)

#### FR-TOPIC-04: Delete Topic
- Xác nhận trước khi xóa
- Audit log

#### FR-TOPIC-05: Update Topic Configs
- List current configs với default/override indicator
- Update config values
- Reset config to default

#### FR-TOPIC-06: Increase Partitions
- Chỉ cho phép tăng, không giảm
- Warning về partition rebalancing

#### FR-TOPIC-07: Topic ACLs
- Hiển thị ACLs liên quan đến topic

#### FR-TOPIC-08: Topic Flow Diagram
- Mermaid diagram hiển thị:
  - Topic với partition count
  - Consumer groups đang consume
  - Lag của mỗi consumer group

---

### 2.4 Message Browser

#### FR-MSG-01: Browse Messages
- Sort options:
  - OLDEST: Từ đầu topic
  - NEWEST: Từ cuối topic
- Filter by:
  - Partition
  - Timestamp range
  - Key contains
  - Value contains
  - Header key/value
- Pagination với cursor-based

#### FR-MSG-02: Message Details
- Partition, Offset, Timestamp
- Key (với format detection)
- Value (với format detection: String, JSON, Avro, Protobuf)
- Headers
- Schema info (nếu có)

#### FR-MSG-03: Produce Message
- Input: key, value, headers
- Optional: partition, timestamp
- Schema selection (nếu dùng Schema Registry)
- Validate schema trước khi produce

#### FR-MSG-04: Delete Message (Tombstone)
- Produce message với value = null
- Chỉ áp dụng cho compacted topics

#### FR-MSG-05: Empty Topic
- Xóa tất cả messages trong topic
- Sử dụng `AdminClient.deleteRecords()`
- Require confirmation

#### FR-MSG-06: Real-time Tail (SSE)
- Stream messages mới real-time
- Filter by partition
- Stop/Start streaming

#### FR-MSG-07: Export Messages
- Export ra CSV hoặc JSON
- Filter options giống Browse
- Limit số records

#### FR-MSG-08: Copy Data
- Copy messages từ source topic đến destination topic
- Hỗ trợ cross-cluster copy
- Options:
  - Key filter
  - Timestamp range
  - Limit records

---

### 2.5 Consumer Group Management

#### FR-CG-01: List Consumer Groups
- Danh sách consumer groups
- Search by name
- Filter by state (STABLE, EMPTY, PREPARING_REBALANCE, etc.)

#### FR-CG-02: Consumer Group Details
- Group ID, State, Coordinator
- Partition Assignor
- Danh sách topics đang consume

#### FR-CG-03: Consumer Group Members
- Member ID, Client ID, Host
- Assigned partitions

#### FR-CG-04: Consumer Group Offsets
- Per partition: current offset, end offset, lag
- Total lag

#### FR-CG-05: Reset Offsets
- Strategies:
  - To beginning
  - To end
  - To specific offset
  - To timestamp
  - Shift by N (forward/backward)
- Require group ở trạng thái EMPTY

#### FR-CG-06: Delete Consumer Group
- Xác nhận trước khi xóa
- Chỉ xóa được khi group EMPTY

#### FR-CG-07: Delete Consumer Group Offsets
- Xóa offsets cho specific topic
- Require group ở trạng thái EMPTY

#### FR-CG-08: Consumer Group Diagram
- Mermaid diagram hiển thị:
  - Consumer group với members
  - Topics đang consume
  - Lag per partition

---

### 2.6 Schema Registry

#### FR-SCHEMA-01: List Schemas (Subjects)
- Danh sách subjects
- Search by name
- Filter by schema type (AVRO, JSON, PROTOBUF)

#### FR-SCHEMA-02: Schema Details
- Subject name
- Latest version
- Schema type
- Schema content (formatted)
- Compatibility level

#### FR-SCHEMA-03: Schema Versions
- List all versions
- Compare versions
- View specific version

#### FR-SCHEMA-04: Register Schema
- Input: subject, schema type, schema content
- Validate schema syntax
- Check compatibility

#### FR-SCHEMA-05: Update Schema
- Register new version
- Auto-check compatibility

#### FR-SCHEMA-06: Delete Schema
- Delete all versions
- Delete specific version
- Soft delete vs Hard delete

#### FR-SCHEMA-07: Compatibility Config
- View current compatibility level
- Update compatibility level
- Levels: BACKWARD, FORWARD, FULL, NONE, etc.

#### FR-SCHEMA-08: Test Compatibility
- Test schema against existing versions

---

### 2.7 Kafka Connect

#### FR-CONNECT-01: List Connectors
- Danh sách connectors
- Status: RUNNING, PAUSED, FAILED
- Type: SOURCE, SINK

#### FR-CONNECT-02: Connector Details
- Name, Type, Status
- Config
- Tasks với status

#### FR-CONNECT-03: Create Connector
- Input: name, config
- Validate config với plugin

#### FR-CONNECT-04: Update Connector Config
- Edit config
- Validate trước khi apply

#### FR-CONNECT-05: Delete Connector
- Xác nhận trước khi xóa

#### FR-CONNECT-06: Connector Actions
- Pause connector
- Resume connector
- Restart connector
- Restart specific task

#### FR-CONNECT-07: List Plugins
- Danh sách available plugins
- Plugin config definition

---

### 2.8 ACL Management

#### FR-ACL-01: List ACLs
- Danh sách ACLs
- Filter by:
  - Resource type (TOPIC, GROUP, CLUSTER, etc.)
  - Principal
  - Operation

#### FR-ACL-02: Create ACL
- Input:
  - Resource type, Resource name, Pattern type
  - Principal
  - Host
  - Operation
  - Permission type (ALLOW/DENY)

#### FR-ACL-03: Delete ACL
- Xác nhận trước khi xóa

---

### 2.9 User Management

#### FR-USER-01: List Users
- Danh sách users
- Search by username, email
- Filter by role

#### FR-USER-02: Create User
- Input: username, email, password, full name
- Assign roles

#### FR-USER-03: Update User
- Update profile info
- Change password
- Update roles

#### FR-USER-04: Delete User
- Soft delete (deactivate)
- Cannot delete admin user

#### FR-USER-05: User Permissions View
- Xem tất cả permissions của user (aggregated từ roles)

---

### 2.10 Role Management

#### FR-ROLE-01: List Roles
- Danh sách roles
- System roles (không thể xóa): admin, viewer

#### FR-ROLE-02: Create Role
- Input: name, description
- Assign permissions

#### FR-ROLE-03: Update Role
- Update name, description
- Add/Remove permissions

#### FR-ROLE-04: Delete Role
- Không thể xóa system roles
- Warning nếu role đang được assign cho users

---

### 2.11 Permission Management

#### FR-PERM-01: List Permissions
- Danh sách permissions
- Filter by resource type

#### FR-PERM-02: Create Permission
- Input:
  - Resource type
  - Action
  - Resource pattern (optional)
  - Cluster IDs (optional)
  - Description

#### FR-PERM-03: Update Permission
- Update tất cả fields

#### FR-PERM-04: Delete Permission
- Warning nếu permission đang được assign cho roles

---

### 2.12 Audit Logging

#### FR-AUDIT-01: Log All Actions
- User actions: login, logout
- CRUD operations trên tất cả resources
- Log format: timestamp, user, action, resource, details, IP

#### FR-AUDIT-02: Audit Log Storage
- File-based (Logback)
- Optional: Push to ELK stack

#### FR-AUDIT-03: Audit Log Viewer (Optional Phase 2)
- View audit logs trong UI
- Search, filter

---

## 3. Non-Functional Requirements

### 3.1 Performance

#### NFR-PERF-01: Response Time
- API response < 500ms cho list operations
- API response < 200ms cho single resource operations

#### NFR-PERF-02: Caching
- Local cache: Caffeine
- Distributed cache: Redis
- Cache TTL:
  - Topic list: 30 seconds
  - Topic configs: 5 minutes
  - Cluster info: 1 minute

#### NFR-PERF-03: Concurrent Users
- Support 50 concurrent users

### 3.2 Security

#### NFR-SEC-01: Authentication
- JWT-based authentication
- Password hashing: BCrypt

#### NFR-SEC-02: Authorization
- RBAC với fine-grained permissions
- API-level authorization

#### NFR-SEC-03: Input Validation
- Validate all inputs
- Prevent SQL injection, XSS

### 3.3 Reliability

#### NFR-REL-01: Error Handling
- Graceful error handling
- Meaningful error messages

#### NFR-REL-02: Connection Management
- Kafka client connection pooling
- Auto-reconnect on failure

### 3.4 Maintainability

#### NFR-MAIN-01: Code Structure
- Clean Architecture / Hexagonal Architecture
- Separation of concerns

#### NFR-MAIN-02: Documentation
- API documentation (OpenAPI/Swagger)
- Code comments

#### NFR-MAIN-03: Testing
- Unit tests
- Integration tests

---

## 4. Technical Architecture

### 4.1 Backend Stack
- **Language**: Java 21
- **Framework**: Spring Boot 3.x
- **Build Tool**: Maven
- **Database**: PostgreSQL (users, roles, permissions, cluster configs)
- **Cache**: Redis + Caffeine
- **Migration**: Flyway

### 4.2 Frontend Stack
- **Framework**: React 18
- **Language**: TypeScript
- **UI Library**: Ant Design 5.x
- **State Management**: Zustand
- **Data Fetching**: TanStack Query (React Query)
- **Charts**: Mermaid.js
- **Build Tool**: Vite

### 4.3 Infrastructure
- **Containerization**: Docker
- **Orchestration**: Docker Compose (dev), Kubernetes (prod)

---

## 5. Development Phases

### Phase 1 - MVP (4-6 weeks)
1. Project setup (Backend + Frontend)
2. Authentication (JWT)
3. User & Role Management (basic RBAC)
4. Cluster Management
5. Topic Management (CRUD, configs)
6. Message Browser (browse, produce)
7. Consumer Group Management
8. Schema Registry

### Phase 2 - Enhanced Features (3-4 weeks)
1. Real-time Tail (SSE)
2. Export/Download messages
3. Copy data between topics/clusters
4. Kafka Connect management
5. ACL Management
6. Mermaid Diagrams
7. Advanced RBAC (resource patterns, cluster scope)

### Phase 3 - Polish (2-3 weeks)
1. Audit Logging
2. Performance optimization
3. UI/UX improvements
4. Documentation
5. Testing

---

## 6. API Endpoints Summary

### Authentication
| Method | Endpoint | Description |
|--------|----------|-------------|
| POST | `/api/v1/auth/login` | Login |
| POST | `/api/v1/auth/refresh` | Refresh token |
| POST | `/api/v1/auth/logout` | Logout |

### Clusters
| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/api/v1/clusters` | List clusters |
| GET | `/api/v1/clusters/{id}` | Get cluster details |
| GET | `/api/v1/clusters/{id}/nodes` | List nodes |
| GET | `/api/v1/clusters/{id}/nodes/{nodeId}` | Get node details |
| GET | `/api/v1/clusters/{id}/nodes/{nodeId}/configs` | Get node configs |

### Topics
| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/api/v1/clusters/{id}/topics` | List topics |
| POST | `/api/v1/clusters/{id}/topics` | Create topic |
| GET | `/api/v1/clusters/{id}/topics/{name}` | Get topic details |
| DELETE | `/api/v1/clusters/{id}/topics/{name}` | Delete topic |
| GET | `/api/v1/clusters/{id}/topics/{name}/configs` | Get topic configs |
| PUT | `/api/v1/clusters/{id}/topics/{name}/configs` | Update topic configs |
| POST | `/api/v1/clusters/{id}/topics/{name}/partitions` | Increase partitions |

### Messages
| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/api/v1/clusters/{id}/topics/{name}/messages` | Browse messages |
| POST | `/api/v1/clusters/{id}/topics/{name}/messages` | Produce message |
| DELETE | `/api/v1/clusters/{id}/topics/{name}/messages` | Delete message (tombstone) |
| DELETE | `/api/v1/clusters/{id}/topics/{name}/messages/all` | Empty topic |
| GET | `/api/v1/clusters/{id}/topics/{name}/messages/tail` | Real-time tail (SSE) |
| GET | `/api/v1/clusters/{id}/topics/{name}/messages/export` | Export messages |
| POST | `/api/v1/clusters/{id}/topics/{name}/messages/copy` | Copy messages |

### Consumer Groups
| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/api/v1/clusters/{id}/consumer-groups` | List consumer groups |
| GET | `/api/v1/clusters/{id}/consumer-groups/{groupId}` | Get group details |
| DELETE | `/api/v1/clusters/{id}/consumer-groups/{groupId}` | Delete group |
| GET | `/api/v1/clusters/{id}/consumer-groups/{groupId}/offsets` | Get offsets |
| POST | `/api/v1/clusters/{id}/consumer-groups/{groupId}/offsets/reset` | Reset offsets |

### Schema Registry
| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/api/v1/clusters/{id}/schemas` | List schemas |
| POST | `/api/v1/clusters/{id}/schemas` | Register schema |
| GET | `/api/v1/clusters/{id}/schemas/{subject}` | Get schema |
| DELETE | `/api/v1/clusters/{id}/schemas/{subject}` | Delete schema |
| GET | `/api/v1/clusters/{id}/schemas/{subject}/versions` | List versions |
| GET | `/api/v1/clusters/{id}/schemas/{subject}/config` | Get compatibility |
| PUT | `/api/v1/clusters/{id}/schemas/{subject}/config` | Update compatibility |

### Kafka Connect
| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/api/v1/clusters/{id}/connectors` | List connectors |
| POST | `/api/v1/clusters/{id}/connectors` | Create connector |
| GET | `/api/v1/clusters/{id}/connectors/{name}` | Get connector |
| PUT | `/api/v1/clusters/{id}/connectors/{name}/config` | Update config |
| DELETE | `/api/v1/clusters/{id}/connectors/{name}` | Delete connector |
| POST | `/api/v1/clusters/{id}/connectors/{name}/restart` | Restart connector |
| PUT | `/api/v1/clusters/{id}/connectors/{name}/pause` | Pause connector |
| PUT | `/api/v1/clusters/{id}/connectors/{name}/resume` | Resume connector |

### ACLs
| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/api/v1/clusters/{id}/acls` | List ACLs |
| POST | `/api/v1/clusters/{id}/acls` | Create ACL |
| DELETE | `/api/v1/clusters/{id}/acls` | Delete ACL |

### Users
| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/api/v1/users` | List users |
| POST | `/api/v1/users` | Create user |
| GET | `/api/v1/users/{id}` | Get user |
| PUT | `/api/v1/users/{id}` | Update user |
| DELETE | `/api/v1/users/{id}` | Delete user |
| PUT | `/api/v1/users/{id}/roles` | Assign roles |

### Roles
| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/api/v1/roles` | List roles |
| POST | `/api/v1/roles` | Create role |
| GET | `/api/v1/roles/{id}` | Get role |
| PUT | `/api/v1/roles/{id}` | Update role |
| DELETE | `/api/v1/roles/{id}` | Delete role |
| PUT | `/api/v1/roles/{id}/permissions` | Assign permissions |

### Permissions
| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/api/v1/permissions` | List permissions |
| POST | `/api/v1/permissions` | Create permission |
| PUT | `/api/v1/permissions/{id}` | Update permission |
| DELETE | `/api/v1/permissions/{id}` | Delete permission |

---

## 7. Database Schema

### Tables
- `users` - User accounts
- `roles` - Role definitions
- `permissions` - Permission definitions
- `user_roles` - User-Role mapping (M:N)
- `role_permissions` - Role-Permission mapping (M:N)
- `permission_clusters` - Permission-Cluster mapping
- `cluster_configs` - Cluster connection configs (optional, có thể dùng YAML)
- `audit_logs` - Audit log entries (optional)

---

## 8. Acceptance Criteria

### Phase 1 Completion
- [ ] User có thể login và nhận JWT token
- [ ] Admin có thể CRUD users, roles, permissions
- [ ] User có thể xem clusters được phân quyền
- [ ] User có thể CRUD topics (theo permission)
- [ ] User có thể browse và produce messages
- [ ] User có thể xem và reset consumer group offsets
- [ ] User có thể CRUD schemas trong Schema Registry

### Phase 2 Completion
- [ ] User có thể tail messages real-time
- [ ] User có thể export messages ra CSV/JSON
- [ ] User có thể copy messages giữa topics/clusters
- [ ] User có thể quản lý Kafka Connect connectors
- [ ] User có thể quản lý Kafka ACLs
- [ ] Mermaid diagrams hiển thị đúng topology

### Phase 3 Completion
- [ ] Audit logs được ghi cho tất cả actions
- [ ] Performance đạt yêu cầu NFR
- [ ] Documentation hoàn chỉnh
- [ ] Test coverage > 70%
