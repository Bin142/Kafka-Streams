-- Default admin user (password: admin123)
-- BCrypt hash for 'admin123' generated with BCryptPasswordEncoder
INSERT INTO users (id, username, email, password_hash, full_name, is_admin, is_active, created_at, updated_at)
VALUES (1, 'admin', 'admin@kafkamanagement.local', 
        '$2a$10$9ZXUqQTunMlu7mwb/zhK5eahINXBdoY3J164gjFkg1ELHlp4evzgq', 
        'System Administrator', TRUE, TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP);

-- Default roles
INSERT INTO roles (id, name, description, is_system, created_at, updated_at) VALUES 
    (1, 'ADMIN', 'Full system access', TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
    (2, 'DEVELOPER', 'Can manage topics, consumer groups, and schemas', TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
    (3, 'VIEWER', 'Read-only access', TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP);

-- Default permissions
INSERT INTO permissions (id, resource, action, description, created_at) VALUES 
    (1, 'CLUSTER', 'MANAGE', 'Full access to all clusters', CURRENT_TIMESTAMP),
    (2, 'TOPIC', 'MANAGE', 'Full access to all topics', CURRENT_TIMESTAMP),
    (3, 'TOPIC_DATA', 'MANAGE', 'Full access to topic data', CURRENT_TIMESTAMP),
    (4, 'CONSUMER_GROUP', 'MANAGE', 'Full access to consumer groups', CURRENT_TIMESTAMP),
    (5, 'SCHEMA', 'MANAGE', 'Full access to schemas', CURRENT_TIMESTAMP),
    (6, 'CONNECT', 'MANAGE', 'Full access to Kafka Connect', CURRENT_TIMESTAMP),
    (7, 'ACL', 'MANAGE', 'Full access to ACLs', CURRENT_TIMESTAMP),
    (8, 'USER', 'MANAGE', 'Full access to users', CURRENT_TIMESTAMP),
    (9, 'ROLE', 'MANAGE', 'Full access to roles', CURRENT_TIMESTAMP);

-- Assign admin role to admin user
INSERT INTO user_roles (user_id, role_id) VALUES (1, 1);

-- Assign all permissions to admin role
INSERT INTO role_permissions (role_id, permission_id) VALUES 
    (1, 1), (1, 2), (1, 3), (1, 4), (1, 5), (1, 6), (1, 7), (1, 8), (1, 9);
