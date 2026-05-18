-- Roles table
CREATE TABLE roles (
    id BIGSERIAL PRIMARY KEY,
    name VARCHAR(50) NOT NULL UNIQUE,
    description VARCHAR(255),
    is_system BOOLEAN DEFAULT FALSE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Index
CREATE INDEX idx_roles_name ON roles(name);

-- Insert default system roles
INSERT INTO roles (name, description, is_system) VALUES 
    ('ADMIN', 'Full system access', TRUE),
    ('DEVELOPER', 'Can manage topics, consumer groups, and schemas', TRUE),
    ('VIEWER', 'Read-only access', TRUE);
