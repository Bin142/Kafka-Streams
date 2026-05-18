// Cluster Types
export interface Cluster {
  id: string;
  name: string;
  bootstrapServers: string;
  status: 'CONNECTED' | 'DISCONNECTED' | 'ERROR';
  nodeCount: number;
  hasSchemaRegistry?: boolean;
  hasKafkaConnect?: boolean;
  schemaRegistryUrl?: string;
  kafkaConnectUrl?: string;
}

export interface ClusterDetail extends Cluster {
  clusterId: string;
  controller: Node;
  nodes: Node[];
}

export interface Node {
  id: number;
  host: string;
  port: number;
  rack?: string;
}

// Topic Types
export interface Topic {
  name: string;
  partitionCount: number;
  replicationFactor: number;
  internal: boolean;
  messageCount: number;
}

export interface TopicDetail extends Topic {
  partitions: Partition[];
}

export interface Partition {
  partition: number;
  leader: number;
  replicas: number[];
  isr: number[];
  beginningOffset: number;
  endOffset: number;
  messageCount: number;
}

export interface TopicConfig {
  name: string;
  value: string;
  isDefault: boolean;
  isReadOnly: boolean;
  isSensitive: boolean;
  source: string;
  documentation?: string;
}

export interface TopicCreateRequest {
  name: string;
  partitions: number;
  replicationFactor: number;
  configs?: Record<string, string>;
}

// Consumer Group Types
export interface ConsumerGroup {
  groupId: string;
  state: string;
  members: number;
  topics: string[];
  totalLag: number;
}

export interface ConsumerGroupDetail extends ConsumerGroup {
  coordinator: Node;
  partitionAssignor: string;
  offsets: ConsumerGroupOffset[];
}

export interface ConsumerGroupOffset {
  topic: string;
  partition: number;
  currentOffset: number;
  endOffset: number;
  lag: number;
  consumerId?: string;
  host?: string;
}

// Message Types
export interface Message {
  partition: number;
  offset: number;
  timestamp: string;
  timestampType: string;
  key: string | null;
  value: string | null;
  headers: Record<string, string>;
  keyFormat: string;
  valueFormat: string;
}

export interface MessageBrowseResponse {
  messages: Message[];
  nextCursor?: string;
  previousCursor?: string;
  hasMore: boolean;
  totalPartitions: number;
  scannedMessages: number;
}

export interface MessageProduceRequest {
  key?: string;
  value: string;
  headers?: Record<string, string>;
  partition?: number;
}

// Schema Types
export interface Schema {
  subject: string;
  version: number;
  id: number;
  schemaType: string;
  schema: string;
  compatibility: string;
}

// User Types
export interface User {
  id: number;
  username: string;
  email: string;
  fullName: string;
  isAdmin: boolean;
  isActive: boolean;
  roles: string[];
  createdAt: string;
  updatedAt: string;
}

export interface Role {
  id: number;
  name: string;
  description: string;
  isSystem: boolean;
  permissions: Permission[];
  createdAt: string;
  updatedAt: string;
}

export interface Permission {
  id: number;
  resource: string;
  action: string;
  resourcePattern?: string;
  clusterIds: string[];
  description: string;
  createdAt: string;
}

// Pagination
export interface Page<T> {
  content: T[];
  totalElements: number;
  totalPages: number;
  size: number;
  number: number;
  first: boolean;
  last: boolean;
}

// API Error
export interface ApiError {
  errorCode: string;
  message: string;
  details?: string;
  timestamp: string;
}
