import apiClient from './client';
import type { Cluster, ClusterDetail, Node, TopicConfig } from './types';

export const clusterApi = {
  // List all clusters
  listClusters: async (): Promise<Cluster[]> => {
    const response = await apiClient.get('/v1/clusters');
    return response.data;
  },

  // Get cluster details
  getCluster: async (clusterId: string): Promise<ClusterDetail> => {
    const response = await apiClient.get(`/v1/clusters/${clusterId}`);
    return response.data;
  },

  // Create cluster
  createCluster: async (data: Partial<Cluster>): Promise<Cluster> => {
    const response = await apiClient.post('/v1/clusters', data);
    return response.data;
  },

  // Delete cluster
  deleteCluster: async (clusterId: string): Promise<void> => {
    await apiClient.delete(`/v1/clusters/${clusterId}`);
  },

  // List nodes in cluster
  listNodes: async (clusterId: string): Promise<Node[]> => {
    const response = await apiClient.get(`/v1/clusters/${clusterId}/nodes`);
    return response.data;
  },

  // Get node details
  getNode: async (clusterId: string, nodeId: number): Promise<Node> => {
    const response = await apiClient.get(`/v1/clusters/${clusterId}/nodes/${nodeId}`);
    return response.data;
  },

  // Get node configs
  getNodeConfigs: async (clusterId: string, nodeId: number): Promise<TopicConfig[]> => {
    const response = await apiClient.get(`/v1/clusters/${clusterId}/nodes/${nodeId}/configs`);
    return response.data;
  },
};
