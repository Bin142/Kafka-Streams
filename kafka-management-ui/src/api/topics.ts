import apiClient from './client';
import type { Topic, TopicDetail, TopicConfig, TopicCreateRequest } from './types';

export const topicApi = {
  // List topics
  listTopics: async (
    clusterId: string,
    params?: {
      page?: number;
      size?: number;
      search?: string;
      view?: 'ALL' | 'HIDE_INTERNAL' | 'HIDE_STREAM' | 'HIDE_INTERNAL_STREAM';
    }
  ): Promise<Topic[]> => {
    const response = await apiClient.get(`/v1/clusters/${clusterId}/topics`, { params });
    // Handle both paginated and non-paginated responses
    return response.data.content || response.data;
  },

  // Get topic details
  getTopic: async (clusterId: string, topicName: string): Promise<TopicDetail> => {
    const response = await apiClient.get(`/v1/clusters/${clusterId}/topics/${encodeURIComponent(topicName)}`);
    return response.data;
  },

  // Create topic
  createTopic: async (clusterId: string, data: TopicCreateRequest): Promise<Topic> => {
    const response = await apiClient.post(`/v1/clusters/${clusterId}/topics`, data);
    return response.data;
  },

  // Delete topic
  deleteTopic: async (clusterId: string, topicName: string): Promise<void> => {
    await apiClient.delete(`/v1/clusters/${clusterId}/topics/${encodeURIComponent(topicName)}`);
  },

  // Get topic configs
  getTopicConfigs: async (clusterId: string, topicName: string): Promise<TopicConfig[]> => {
    const response = await apiClient.get(`/v1/clusters/${clusterId}/topics/${encodeURIComponent(topicName)}/configs`);
    return response.data;
  },

  // Update topic configs
  updateTopicConfigs: async (
    clusterId: string,
    topicName: string,
    configs: Record<string, string>
  ): Promise<void> => {
    await apiClient.put(`/v1/clusters/${clusterId}/topics/${encodeURIComponent(topicName)}/configs`, configs);
  },

  // Increase partitions
  increasePartitions: async (
    clusterId: string,
    topicName: string,
    newPartitionCount: number
  ): Promise<void> => {
    await apiClient.post(`/v1/clusters/${clusterId}/topics/${encodeURIComponent(topicName)}/partitions`, null, {
      params: { count: newPartitionCount },
    });
  },
};
