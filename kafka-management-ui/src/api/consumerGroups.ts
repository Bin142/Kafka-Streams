import apiClient from './client';
import type { ConsumerGroup, ConsumerGroupDetail } from './types';

export const consumerGroupApi = {
  // List consumer groups
  listConsumerGroups: async (
    clusterId: string,
    params?: {
      page?: number;
      size?: number;
      search?: string;
      state?: string;
    }
  ): Promise<ConsumerGroup[]> => {
    const response = await apiClient.get(`/v1/clusters/${clusterId}/consumer-groups`, { params });
    // Handle both paginated and non-paginated responses
    return response.data.content || response.data;
  },

  // Get consumer group details
  getConsumerGroup: async (clusterId: string, groupId: string): Promise<ConsumerGroupDetail> => {
    const response = await apiClient.get(`/v1/clusters/${clusterId}/consumer-groups/${encodeURIComponent(groupId)}`);
    return response.data;
  },

  // Delete consumer group
  deleteConsumerGroup: async (clusterId: string, groupId: string): Promise<void> => {
    await apiClient.delete(`/v1/clusters/${clusterId}/consumer-groups/${encodeURIComponent(groupId)}`);
  },

  // Reset offsets
  resetOffsets: async (
    clusterId: string,
    groupId: string,
    topic: string,
    strategy: string,
    value?: number
  ): Promise<void> => {
    await apiClient.post(
      `/v1/clusters/${clusterId}/consumer-groups/${encodeURIComponent(groupId)}/offsets/reset`,
      {
        topic,
        strategy,
        offset: strategy === 'TO_OFFSET' ? value : undefined,
        shiftBy: strategy === 'SHIFT_BY' ? value : undefined,
        timestamp: strategy === 'TO_DATETIME' ? value : undefined,
      }
    );
  },
};
