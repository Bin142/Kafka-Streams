-- Role-Permission mapping table (Many-to-Many)
CREATE TABLE role_permissions (
    role_id BIGINT NOT NULL REFERENCES roles(id) ON DELETE CASCADE,
    permission_id BIGINT NOT NULL REFERENCES permissions(id) ON DELETE CASCADE,
    PRIMARY KEY (role_id, permission_id)
);

-- Indexes
CREATE INDEX idx_role_permissions_role_id ON role_permissions(role_id);
CREATE INDEX idx_role_permissions_permission_id ON role_permissions(permission_id);

-- Assign all MANAGE permissions to ADMIN role
INSERT INTO role_permissions (role_id, permission_id)
SELECT r.id, p.id FROM roles r, permissions p 
WHERE r.name = 'ADMIN' AND p.action = 'MANAGE';

-- Assign DEVELOPER permissions to DEVELOPER role
INSERT INTO role_permissions (role_id, permission_id)
SELECT r.id, p.id FROM roles r, permissions p 
WHERE r.name = 'DEVELOPER' 
AND p.description LIKE '%' 
AND p.action != 'MANAGE'
AND p.id NOT IN (
    SELECT id FROM permissions WHERE description LIKE '%(viewer)%'
);

-- Assign VIEWER permissions to VIEWER role
INSERT INTO role_permissions (role_id, permission_id)
SELECT r.id, p.id FROM roles r, permissions p 
WHERE r.name = 'VIEWER' AND p.description LIKE '%(viewer)%';
