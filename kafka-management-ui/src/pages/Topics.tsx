import React, { useState, useMemo } from 'react';
import { useNavigate } from 'react-router-dom';
import {
  Table,
  Button,
  Input,
  Space,
  Tag,
  Typography,
  Modal,
  Form,
  InputNumber,
  message,
  Tooltip,
  Card,
  Alert,
  Badge,
} from 'antd';
import {
  PlusOutlined,
  SearchOutlined,
  ReloadOutlined,
  EyeOutlined,
  DatabaseOutlined,
  DisconnectOutlined,
  TeamOutlined,
} from '@ant-design/icons';
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query';
import { useClusterStore } from '../store/clusterStore';
import { topicApi, consumerGroupApi } from '../api';
import { mockTopics, mockConsumerGroups } from '../api/mockData';
import type { Topic, TopicCreateRequest, ConsumerGroup } from '../api/types';

const { Title } = Typography;

const Topics: React.FC = () => {
  const navigate = useNavigate();
  const queryClient = useQueryClient();
  const { selectedClusterId, getSelectedCluster } = useClusterStore();
  const cluster = getSelectedCluster();
  const [searchText, setSearchText] = useState('');
  const [createModalOpen, setCreateModalOpen] = useState(false);
  const [form] = Form.useForm();

  const isConnected = cluster?.status === 'CONNECTED';

  const { data: topics, isLoading, error, refetch } = useQuery({
    queryKey: ['topics', selectedClusterId],
    queryFn: () => topicApi.listTopics(selectedClusterId!),
    enabled: !!selectedClusterId && isConnected,
    retry: false,
    staleTime: 30000,
  });

  // Fetch consumer groups to show which groups are listening to each topic
  const { data: consumerGroups } = useQuery({
    queryKey: ['consumerGroups', selectedClusterId],
    queryFn: () => consumerGroupApi.listConsumerGroups(selectedClusterId!),
    enabled: !!selectedClusterId && isConnected,
    retry: false,
    staleTime: 30000,
  });

  // Use mock data when disconnected or error
  const displayTopics = isConnected && !error ? (topics || []) : mockTopics;
  const displayGroups = isConnected && !error ? (consumerGroups || []) : mockConsumerGroups;

  // Build a map of topic -> consumer groups
  const topicConsumerMap = useMemo(() => {
    const map: Record<string, ConsumerGroup[]> = {};
    displayGroups.forEach((group: ConsumerGroup) => {
      group.topics.forEach((topicName: string) => {
        if (!map[topicName]) {
          map[topicName] = [];
        }
        map[topicName].push(group);
      });
    });
    return map;
  }, [displayGroups]);

  const createMutation = useMutation({
    mutationFn: (data: TopicCreateRequest) => 
      topicApi.createTopic(selectedClusterId!, data),
    onSuccess: () => {
      message.success('Topic created successfully');
      setCreateModalOpen(false);
      form.resetFields();
      queryClient.invalidateQueries({ queryKey: ['topics', selectedClusterId] });
    },
    onError: (error: Error) => {
      message.error(`Failed to create topic: ${error.message}`);
    },
  });

  const filteredTopics = displayTopics.filter((topic: Topic) =>
    topic.name.toLowerCase().includes(searchText.toLowerCase())
  );

  const columns = [
    {
      title: 'Topic Name',
      dataIndex: 'name',
      key: 'name',
      sorter: (a: Topic, b: Topic) => a.name.localeCompare(b.name),
      render: (name: string, record: Topic) => (
        <Space>
          <DatabaseOutlined style={{ color: record.internal ? '#faad14' : '#1890ff' }} />
          <a onClick={() => navigate(`/topics/${encodeURIComponent(name)}`)}>{name}</a>
          {record.internal && <Tag color="warning">Internal</Tag>}
        </Space>
      ),
    },
    {
      title: 'Partitions',
      dataIndex: 'partitionCount',
      key: 'partitionCount',
      width: 120,
      sorter: (a: Topic, b: Topic) => a.partitionCount - b.partitionCount,
    },
    {
      title: 'Replication Factor',
      dataIndex: 'replicationFactor',
      key: 'replicationFactor',
      width: 160,
    },
    {
      title: 'Messages',
      dataIndex: 'messageCount',
      key: 'messageCount',
      width: 120,
      sorter: (a: Topic, b: Topic) => a.messageCount - b.messageCount,
      render: (count: number) => count?.toLocaleString() || '-',
    },
    {
      title: (
        <Space>
          <TeamOutlined />
          Consumer Groups
        </Space>
      ),
      key: 'consumerGroups',
      width: 280,
      render: (_: unknown, record: Topic) => {
        const groups = topicConsumerMap[record.name] || [];
        if (groups.length === 0) {
          return <span style={{ color: '#999' }}>No consumers</span>;
        }
        return (
          <Space size={[4, 4]} wrap>
            {groups.slice(0, 3).map((group: ConsumerGroup) => {
              const stateColor = group.state === 'STABLE' ? 'success' : 
                                 group.state === 'EMPTY' ? 'default' : 'processing';
              return (
                <Tooltip 
                  key={group.groupId} 
                  title={
                    <div>
                      <div>State: {group.state}</div>
                      <div>Members: {group.members}</div>
                      <div>Total Lag: {group.totalLag.toLocaleString()}</div>
                    </div>
                  }
                >
                  <Tag 
                    style={{ cursor: 'pointer', marginRight: 0 }}
                    onClick={() => navigate(`/consumer-groups/${encodeURIComponent(group.groupId)}`)}
                  >
                    <Badge status={stateColor} />
                    {group.groupId}
                    {group.totalLag > 0 && (
                      <span style={{ marginLeft: 4, color: group.totalLag > 1000 ? '#ff4d4f' : '#faad14' }}>
                        ({group.totalLag > 1000 ? `${(group.totalLag / 1000).toFixed(1)}k` : group.totalLag})
                      </span>
                    )}
                  </Tag>
                </Tooltip>
              );
            })}
            {groups.length > 3 && (
              <Tooltip title={groups.slice(3).map(g => g.groupId).join(', ')}>
                <Tag>+{groups.length - 3} more</Tag>
              </Tooltip>
            )}
          </Space>
        );
      },
    },
    {
      title: 'Actions',
      key: 'actions',
      width: 80,
      render: (_: unknown, record: Topic) => (
        <Tooltip title="View Details">
          <Button
            type="text"
            icon={<EyeOutlined />}
            onClick={() => navigate(`/topics/${encodeURIComponent(record.name)}`)}
          />
        </Tooltip>
      ),
    },
  ];

  if (!selectedClusterId) {
    return (
      <Card>
        <div style={{ textAlign: 'center', padding: '60px 0' }}>
          <DatabaseOutlined style={{ fontSize: 48, color: '#ccc' }} />
          <Title level={4} style={{ color: '#999', marginTop: 16 }}>
            Select a Cluster
          </Title>
        </div>
      </Card>
    );
  }

  return (
    <div>
      <div style={{ display: 'flex', justifyContent: 'space-between', marginBottom: 16 }}>
        <Title level={4} style={{ margin: 0 }}>Topics</Title>
        <Space>
          <Input
            placeholder="Search topics..."
            prefix={<SearchOutlined />}
            value={searchText}
            onChange={(e) => setSearchText(e.target.value)}
            style={{ width: 250 }}
            allowClear
          />
          <Button icon={<ReloadOutlined />} onClick={() => refetch()} disabled={!isConnected}>
            Refresh
          </Button>
          <Button
            type="primary"
            icon={<PlusOutlined />}
            onClick={() => setCreateModalOpen(true)}
            disabled={!isConnected}
          >
            Create Topic
          </Button>
        </Space>
      </div>

      {!isConnected && (
        <Alert
          message="Cluster Disconnected - Showing Sample Data"
          description="Connect to Kafka broker to see real data and perform operations."
          type="warning"
          showIcon
          icon={<DisconnectOutlined />}
          style={{ marginBottom: 16 }}
        />
      )}

      <Table
        columns={columns}
        dataSource={filteredTopics}
        rowKey="name"
        loading={isLoading && isConnected}
        pagination={{
          showSizeChanger: true,
          showQuickJumper: true,
          showTotal: (total) => `Total ${total} topics`,
        }}
      />

      <Modal
        title="Create New Topic"
        open={createModalOpen}
        onCancel={() => {
          setCreateModalOpen(false);
          form.resetFields();
        }}
        footer={null}
      >
        <Form
          form={form}
          layout="vertical"
          onFinish={(values) => createMutation.mutate(values)}
        >
          <Form.Item
            name="name"
            label="Topic Name"
            rules={[
              { required: true, message: 'Please enter topic name' },
              { pattern: /^[a-zA-Z0-9._-]+$/, message: 'Invalid topic name format' },
            ]}
          >
            <Input placeholder="my-topic" />
          </Form.Item>

          <Form.Item
            name="partitions"
            label="Partitions"
            initialValue={3}
            rules={[{ required: true, message: 'Please enter number of partitions' }]}
          >
            <InputNumber min={1} max={1000} style={{ width: '100%' }} />
          </Form.Item>

          <Form.Item
            name="replicationFactor"
            label="Replication Factor"
            initialValue={1}
            rules={[{ required: true, message: 'Please enter replication factor' }]}
          >
            <InputNumber min={1} max={10} style={{ width: '100%' }} />
          </Form.Item>

          <Form.Item style={{ marginBottom: 0, textAlign: 'right' }}>
            <Space>
              <Button onClick={() => setCreateModalOpen(false)}>Cancel</Button>
              <Button type="primary" htmlType="submit" loading={createMutation.isPending}>
                Create
              </Button>
            </Space>
          </Form.Item>
        </Form>
      </Modal>
    </div>
  );
};

export default Topics;
