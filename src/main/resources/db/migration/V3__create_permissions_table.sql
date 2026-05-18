-- Permissions table
CREATE TABLE permissions (
    id BIGSERIAL PRIMARY KEY,
    resource VARCHAR(50) NOT NULL,
    action VARCHAR(50) NOT NULL,
    resource_pattern VARCHAR(255),
    description VARCHAR(255),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Permission clusters mapping (for cluster-specific permissions)
CREATE TABLE permission_clusters (
    permission_id BIGINT NOT NULL REFERENCES permissions(id) ON DELETE CASCADE,
    cluster_id VARCHAR(100) NOT NULL,
    PRIMARY KEY (permission_id, cluster_id)
);

-- Indexes
CREATE INDEX idx_permissions_resource ON permissions(resource);
CREATE INDEX idx_permissions_action ON permissions(action);
CREATE INDEX idx_permission_clusters_cluster_id ON permission_clusters(cluster_id);

-- Insert default permissions for ADMIN role
INSERT INTO permissions (resource, action, description) VALUES 
    -- Cluster permissions
    ('CLUSTER', 'MANAGE', 'Full access to all clusters'),
    -- Topic permissions
    ('TOPIC', 'MANAGE', 'Full access to all topics'),
    -- Topic data permissions
    ('TOPIC_DATA', 'MANAGE', 'Full access to topic data (browse, produce, delete)'),
    -- Consumer group permissions
    ('CONSUMER_GROUP', 'MANAGE', 'Full access to consumer groups'),
    -- Schema permissions
    ('SCHEMA', 'MANAGE', 'Full access to schemas'),
    -- Connect permissions
    ('CONNECT', 'MANAGE', 'Full access to Kafka Connect'),
    -- ACL permissions
    ('ACL', 'MANAGE', 'Full access to ACLs'),
    -- User permissions
    ('USER', 'MANAGE', 'Full access to users'),
    -- Role permissions
    ('ROLE', 'MANAGE', 'Full access to roles');

-- Insert default permissions for DEVELOPER role
INSERT INTO permissions (resource, action, description) VALUES 
    ('CLUSTER', 'READ', 'View cluster information'),
    ('TOPIC', 'READ', 'View topics'),
    ('TOPIC', 'CREATE', 'Create topics'),
    ('TOPIC', 'UPDATE', 'Update topic configs'),
    ('TOPIC', 'DELETE', 'Delete topics'),
    ('TOPIC_DATA', 'READ', 'Browse messages'),
    ('TOPIC_DATA', 'CREATE', 'Produce messages'),
    ('CONSUMER_GROUP', 'READ', 'View consumer groups'),
    ('CONSUMER_GROUP', 'UPDATE', 'Reset offsets'),
    ('SCHEMA', 'READ', 'View schemas'),
    ('SCHEMA', 'CREATE', 'Register schemas'),
    ('SCHEMA', 'UPDATE', 'Update schemas'),
    ('CONNECT', 'READ', 'View connectors'),
    ('ACL', 'READ', 'View ACLs');

-- Insert default permissions for VIEWER role
INSERT INTO permissions (resource, action, description) VALUES 
    ('CLUSTER', 'READ', 'View cluster information (viewer)'),
    ('TOPIC', 'READ', 'View topics (viewer)'),
    ('TOPIC_DATA', 'READ', 'Browse messages (viewer)'),
    ('CONSUMER_GROUP', 'READ', 'View consumer groups (viewer)'),
    ('SCHEMA', 'READ', 'View schemas (viewer)'),
    ('CONNECT', 'READ', 'View connectors (viewer)'),
    ('ACL', 'READ', 'View ACLs (viewer)');
