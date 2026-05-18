import apiClient from './client';
import type { Message, MessageBrowseResponse, MessageProduceRequest } from './types';

export const messageApi = {
  // Browse messages
  browseMessages: async (
    clusterId: string,
    topicName: string,
    params?: {
      sort?: 'OLDEST' | 'NEWEST';
      partition?: number;
      offset?: number;
      afterTimestamp?: string;
      beforeTimestamp?: string;
      keyContains?: string;
      valueContains?: string;
      limit?: number;
      cursor?: string;
    }
  ): Promise<MessageBrowseResponse> => {
    const response = await apiClient.get(
      `/v1/clusters/${clusterId}/topics/${encodeURIComponent(topicName)}/messages`,
      { params }
    );
    return response.data;
  },

  // Produce message
  produceMessage: async (
    clusterId: string,
    topicName: string,
    data: MessageProduceRequest
  ): Promise<Message> => {
    const response = await apiClient.post(
      `/v1/clusters/${clusterId}/topics/${encodeURIComponent(topicName)}/messages`,
      data
    );
    return response.data;
  },

  // Delete message (tombstone)
  deleteMessage: async (
    clusterId: string,
    topicName: string,
    key: string,
    partition?: number
  ): Promise<void> => {
    await apiClient.delete(`/v1/clusters/${clusterId}/topics/${encodeURIComponent(topicName)}/messages`, {
      params: { key, partition },
    });
  },

  // Empty topic
  emptyTopic: async (clusterId: string, topicName: string): Promise<void> => {
    await apiClient.delete(`/v1/clusters/${clusterId}/topics/${encodeURIComponent(topicName)}/messages/all`);
  },

  // Export messages
  exportMessages: async (
    clusterId: string,
    topicName: string,
    params?: {
      format?: 'JSON' | 'CSV';
      partition?: number;
      limit?: number;
    }
  ): Promise<Blob> => {
    const response = await apiClient.get(
      `/v1/clusters/${clusterId}/topics/${encodeURIComponent(topicName)}/messages/export`,
      {
        params,
        responseType: 'blob',
      }
    );
    return response.data;
  },
};
