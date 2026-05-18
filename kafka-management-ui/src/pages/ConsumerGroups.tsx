import React, { useState } from 'react';
import { useNavigate } from 'react-router-dom';
import {
  Table,
  Button,
  Input,
  Space,
  Tag,
  Typography,
  Tooltip,
  Popconfirm,
  Card,
  message,
  Progress,
  Alert,
} from 'antd';
import {
  SearchOutlined,
  ReloadOutlined,
  DeleteOutlined,
  EyeOutlined,
  TeamOutlined,
  WarningOutlined,
  CheckCircleOutlined,
  DisconnectOutlined,
} from '@ant-design/icons';
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query';
import { useClusterStore } from '../store/clusterStore';
import { consumerGroupApi } from '../api';
import { mockConsumerGroups } from '../api/mockData';
import type { ConsumerGroup } from '../api/types';

const { Title, Text } = Typography;

const ConsumerGroups: React.FC = () => {
  const navigate = useNavigate();
  const queryClient = useQueryClient();
  const { selectedClusterId, getSelectedCluster } = useClusterStore();
  const cluster = getSelectedCluster();
  const [searchText, setSearchText] = useState('');

  const isConnected = cluster?.status === 'CONNECTED';

  const { data: groups, isLoading, error, refetch } = useQuery({
    queryKey: ['consumerGroups', selectedClusterId],
    queryFn: () => consumerGroupApi.listConsumerGroups(selectedClusterId!),
    enabled: !!selectedClusterId && isConnected,
    retry: false,
    staleTime: 30000,
  });

  // Use mock data when disconnected or error
  const displayGroups = isConnected && !error ? (groups || []) : mockConsumerGroups;

  const deleteMutation = useMutation({
    mutationFn: (groupId: string) =>
      consumerGroupApi.deleteConsumerGroup(selectedClusterId!, groupId),
    onSuccess: () => {
      message.success('Consumer group deleted successfully');
      queryClient.invalidateQueries({ queryKey: ['consumerGroups', selectedClusterId] });
    },
    onError: (error: Error) => {
      message.error(`Failed to delete consumer group: ${error.message}`);
    },
  });

  const filteredGroups = displayGroups.filter((group: ConsumerGroup) =>
    group.groupId.toLowerCase().includes(searchText.toLowerCase())
  );

  const getStateColor = (state: string) => {
    switch (state?.toUpperCase()) {
      case 'STABLE':
        return 'success';
      case 'PREPARING_REBALANCE':
      case 'COMPLETING_REBALANCE':
        return 'warning';
      case 'DEAD':
        return 'error';
      case 'EMPTY':
        return 'default';
      default:
        return 'default';
    }
  };

  const getLagStatus = (lag: number) => {
    if (lag === 0) return { color: '#52c41a', status: 'success' as const };
    if (lag < 1000) return { color: '#faad14', status: 'active' as const };
    return { color: '#ff4d4f', status: 'exception' as const };
  };

  const columns = [
    {
      title: 'Group ID',
      dataIndex: 'groupId',
      key: 'groupId',
      sorter: (a: ConsumerGroup, b: ConsumerGroup) => a.groupId.localeCompare(b.groupId),
      render: (groupId: string) => (
        <Space>
          <TeamOutlined style={{ color: '#1890ff' }} />
          <a onClick={() => navigate(`/consumer-groups/${encodeURIComponent(groupId)}`)}>{groupId}</a>
        </Space>
      ),
    },
    {
      title: 'State',
      dataIndex: 'state',
      key: 'state',
      width: 150,
      filters: [
        { text: 'Stable', value: 'STABLE' },
        { text: 'Empty', value: 'EMPTY' },
        { text: 'Dead', value: 'DEAD' },
        { text: 'Rebalancing', value: 'PREPARING_REBALANCE' },
      ],
      onFilter: (value: unknown, record: ConsumerGroup) => record.state === value,
      render: (state: string) => (
        <Tag color={getStateColor(state)}>{state || 'UNKNOWN'}</Tag>
      ),
    },
    {
      title: 'Members',
      dataIndex: 'members',
      key: 'members',
      width: 100,
      sorter: (a: ConsumerGroup, b: ConsumerGroup) => a.members - b.members,
    },
    {
      title: 'Topics',
      dataIndex: 'topics',
      key: 'topics',
      width: 200,
      render: (topics: string[]) => (
        <Space wrap>
          {topics?.slice(0, 3).map((topic) => (
            <Tag key={topic}>{topic}</Tag>
          ))}
          {topics?.length > 3 && <Tag>+{topics.length - 3} more</Tag>}
        </Space>
      ),
    },
    {
      title: 'Total Lag',
      dataIndex: 'totalLag',
      key: 'totalLag',
      width: 200,
      sorter: (a: ConsumerGroup, b: ConsumerGroup) => a.totalLag - b.totalLag,
      render: (lag: number) => {
        const { color, status } = getLagStatus(lag);
        return (
          <Space>
            {lag === 0 ? (
              <CheckCircleOutlined style={{ color }} />
            ) : (
              <WarningOutlined style={{ color }} />
            )}
            <Text strong style={{ color }}>{lag?.toLocaleString() || 0}</Text>
            <Progress
              percent={Math.min((lag / 10000) * 100, 100)}
              size="small"
              status={status}
              showInfo={false}
              style={{ width: 60 }}
            />
          </Space>
        );
      },
    },
    {
      title: 'Actions',
      key: 'actions',
      width: 120,
      render: (_: unknown, record: ConsumerGroup) => (
        <Space>
          <Tooltip title="View Details">
            <Button
              type="text"
              icon={<EyeOutlined />}
              onClick={() => navigate(`/consumer-groups/${encodeURIComponent(record.groupId)}`)}
            />
          </Tooltip>
          <Popconfirm
            title="Delete Consumer Group"
            description={`Are you sure you want to delete "${record.groupId}"?`}
            onConfirm={() => deleteMutation.mutate(record.groupId)}
            okText="Yes"
            cancelText="No"
            okButtonProps={{ danger: true }}
          >
            <Tooltip title="Delete">
              <Button
                type="text"
                danger
                icon={<DeleteOutlined />}
                disabled={record.state === 'STABLE' || !isConnected}
              />
            </Tooltip>
          </Popconfirm>
        </Space>
      ),
    },
  ];

  if (!selectedClusterId) {
    return (
      <Card>
        <div style={{ textAlign: 'center', padding: '60px 0' }}>
          <TeamOutlined style={{ fontSize: 48, color: '#ccc' }} />
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
        <Title level={4} style={{ margin: 0 }}>Consumer Groups</Title>
        <Space>
          <Input
            placeholder="Search consumer groups..."
            prefix={<SearchOutlined />}
            value={searchText}
            onChange={(e) => setSearchText(e.target.value)}
            style={{ width: 280 }}
            allowClear
          />
          <Button icon={<ReloadOutlined />} onClick={() => refetch()} disabled={!isConnected}>
            Refresh
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
        dataSource={filteredGroups}
        rowKey="groupId"
        loading={isLoading && isConnected}
        pagination={{
          showSizeChanger: true,
          showQuickJumper: true,
          showTotal: (total) => `Total ${total} consumer groups`,
        }}
      />
    </div>
  );
};

export default ConsumerGroups;
