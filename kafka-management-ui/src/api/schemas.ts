import apiClient from './client';
import type { Schema } from './types';

export const schemaApi = {
  // List all schemas (subjects)
  listSchemas: async (clusterId: string): Promise<Schema[]> => {
    const response = await apiClient.get(`/v1/clusters/${clusterId}/schemas`);
    return response.data;
  },

  // Get schema by subject
  getSchema: async (clusterId: string, subject: string): Promise<Schema> => {
    const response = await apiClient.get(`/v1/clusters/${clusterId}/schemas/${subject}`);
    return response.data;
  },

  // Get all versions of a schema
  getSchemaVersions: async (clusterId: string, subject: string): Promise<Schema[]> => {
    const response = await apiClient.get(`/v1/clusters/${clusterId}/schemas/${subject}/versions`);
    return response.data;
  },

  // Get specific version
  getSchemaVersion: async (clusterId: string, subject: string, version: number): Promise<Schema> => {
    const response = await apiClient.get(`/v1/clusters/${clusterId}/schemas/${subject}/versions/${version}`);
    return response.data;
  },

  // Create/Register new schema
  createSchema: async (
    clusterId: string,
    subject: string,
    schema: string,
    schemaType: string = 'AVRO'
  ): Promise<Schema> => {
    const response = await apiClient.post(`/v1/clusters/${clusterId}/schemas/${subject}`, {
      schema,
      schemaType,
    });
    return response.data;
  },

  // Delete schema (all versions)
  deleteSchema: async (clusterId: string, subject: string): Promise<void> => {
    await apiClient.delete(`/v1/clusters/${clusterId}/schemas/${subject}`);
  },

  // Delete specific version
  deleteSchemaVersion: async (clusterId: string, subject: string, version: number): Promise<void> => {
    await apiClient.delete(`/v1/clusters/${clusterId}/schemas/${subject}/versions/${version}`);
  },

  // Get compatibility level
  getCompatibility: async (clusterId: string, subject: string): Promise<string> => {
    const response = await apiClient.get(`/v1/clusters/${clusterId}/schemas/${subject}/compatibility`);
    return response.data.compatibility;
  },

  // Set compatibility level
  setCompatibility: async (clusterId: string, subject: string, compatibility: string): Promise<void> => {
    await apiClient.put(`/v1/clusters/${clusterId}/schemas/${subject}/compatibility`, { compatibility });
  },

  // Test compatibility
  testCompatibility: async (clusterId: string, subject: string, schema: string): Promise<boolean> => {
    const response = await apiClient.post(`/v1/clusters/${clusterId}/schemas/${subject}/compatibility/test`, {
      schema,
    });
    return response.data.isCompatible;
  },
};
