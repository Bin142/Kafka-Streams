import apiClient from './client';
import type { Role, Permission } from './types';

export const roleApi = {
  // List all roles
  listRoles: async (): Promise<Role[]> => {
    const response = await apiClient.get('/v1/roles');
    return response.data.content || response.data;
  },

  // Get role by ID
  getRole: async (id: number): Promise<Role> => {
    const response = await apiClient.get(`/v1/roles/${id}`);
    return response.data;
  },

  // Create role
  createRole: async (data: Partial<Role>): Promise<Role> => {
    const response = await apiClient.post('/v1/roles', data);
    return response.data;
  },

  // Update role
  updateRole: async (id: number, data: Partial<Role>): Promise<Role> => {
    const response = await apiClient.put(`/v1/roles/${id}`, data);
    return response.data;
  },

  // Delete role
  deleteRole: async (id: number): Promise<void> => {
    await apiClient.delete(`/v1/roles/${id}`);
  },

  // Assign permissions to role
  assignPermissions: async (id: number, permissionIds: number[]): Promise<void> => {
    await apiClient.put(`/v1/roles/${id}/permissions`, { permissionIds });
  },
};

export const permissionApi = {
  // List all permissions
  listPermissions: async (): Promise<Permission[]> => {
    const response = await apiClient.get('/v1/permissions');
    return response.data.content || response.data;
  },

  // Get permission by ID
  getPermission: async (id: number): Promise<Permission> => {
    const response = await apiClient.get(`/v1/permissions/${id}`);
    return response.data;
  },

  // Create permission
  createPermission: async (data: Partial<Permission>): Promise<Permission> => {
    const response = await apiClient.post('/v1/permissions', data);
    return response.data;
  },

  // Update permission
  updatePermission: async (id: number, data: Partial<Permission>): Promise<Permission> => {
    const response = await apiClient.put(`/v1/permissions/${id}`, data);
    return response.data;
  },

  // Delete permission
  deletePermission: async (id: number): Promise<void> => {
    await apiClient.delete(`/v1/permissions/${id}`);
  },
};
