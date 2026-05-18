import apiClient from './client';
import type { User } from './types';

export const userApi = {
  // List all users
  listUsers: async (params?: { page?: number; size?: number; search?: string }): Promise<User[]> => {
    const response = await apiClient.get('/v1/users', { params });
    // Handle both paginated and non-paginated responses
    return response.data.content || response.data;
  },

  // Get user by ID
  getUser: async (id: number): Promise<User> => {
    const response = await apiClient.get(`/v1/users/${id}`);
    return response.data;
  },

  // Create user
  createUser: async (data: Partial<User> & { password?: string }): Promise<User> => {
    const response = await apiClient.post('/v1/users', data);
    return response.data;
  },

  // Update user
  updateUser: async (id: number, data: Partial<User>): Promise<User> => {
    const response = await apiClient.put(`/v1/users/${id}`, data);
    return response.data;
  },

  // Delete user
  deleteUser: async (id: number): Promise<void> => {
    await apiClient.delete(`/v1/users/${id}`);
  },

  // Change password
  changePassword: async (id: number, oldPassword: string, newPassword: string): Promise<void> => {
    await apiClient.put(`/v1/users/${id}/password`, { oldPassword, newPassword });
  },

  // Assign roles to user
  assignRoles: async (id: number, roleNames: string[]): Promise<void> => {
    await apiClient.put(`/v1/users/${id}/roles`, { roles: roleNames });
  },
};
