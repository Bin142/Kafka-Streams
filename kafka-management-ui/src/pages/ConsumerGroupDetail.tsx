import React, { useState } from 'react';
import { useParams, useNavigate } from 'react-router-dom';
import {
  Card,
  Descriptions,
  Table,
  Button,
  Space,
  Tag,
  Typography,
  Spin,
  Modal,
  Form,
  Select,
  InputNumber,
  message,
  Progress,
  Empty,
  Statistic,
  Row,
  Col,
} from 'antd';
import {
  ArrowLeftOutlined,
  ReloadOutlined,
  RollbackOutlined,
  WarningOutlined,
  CheckCircleOutlined,
} from '@ant-design/icons';
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query';
import { useClusterStore } from '../store/clusterStore';
import { consumerGroupApi } from '../api';
import type { ConsumerGroupOffset } from '../api/types';

const { Title, Text } = Typography;

const ConsumerGroupDetail: React.FC = () => {
  const { groupId } = useParams<{ groupId: string }>();
  const navigate = useNavigate();
  const queryClient = useQueryClient();
  const { selectedClusterId } = useClusterStore();
  const decodedGroupId = decodeURIComponent(groupId || '');

  const [resetModalOpen, setResetModalOpen] = useState(false);
  const [resetForm] = Form.useForm();

  const { data: groupDetail, isLoading, refetch } = useQuery({
    queryKey: ['consumerGroup', selectedClusterId, decodedGroupId],
    queryFn: () => consumerGroupApi.getConsumerGroup(selectedClusterId!, decodedGroupId),
    enabled: !!selectedClusterId && !!decodedGroupId,
  });

  const resetOffsetsMutation = useMutation({
    mutationFn: (data: { topic: string; strategy: string; value?: number }) => {
      if (!selectedClusterId) throw new Error('No cluster selected');
      return consumerGroupApi.resetOffsets(selectedClusterId, decodedGroupId, data.topic, data.strategy, data.value);
    },
    onSuccess: () => {
      message.success('Offsets reset successfully');
      setResetModalOpen(false);
      resetForm.resetFields();
      queryClient.invalidateQueries({ queryKey: ['consumerGroup', selectedClusterId, decodedGroupId] });
    },
    onError: (error: Error) => {
      message.error(`Failed to reset offsets: ${error.message}`);
    },
  });

  const getStateColor = (state?: string) => {
    switch (state?.toUpperCase()) {
      case 'STABLE': return 'success';
      case 'PREPARING_REBALANCE':
      case 'COMPLETING_REBALANCE': return 'warning';
      case 'DEAD': return 'error';
      case 'EMPTY': return 'default';
      default: return 'default';
    }
  };

  const getLagStatus = (lag: number) => {
    if (lag === 0) return { color: '#52c41a', status: 'success' as const };
    if (lag < 1000) return { color: '#faad14', status: 'active' as const };
    return { color: '#ff4d4f', status: 'exception' as const };
  };

  const offsetColumns = [
    { title: 'Topic', dataIndex: 'topic', key: 'topic' },
    { title: 'Partition', dataIndex: 'partition', key: 'partition', width: 100 },
    { title: 'Current Offset', dataIndex: 'currentOffset', key: 'currentOffset', width: 130 },
    { title: 'End Offset', dataIndex: 'endOffset', key: 'endOffset', width: 120 },
    {
      title: 'Lag',
      dataIndex: 'lag',
      key: 'lag',
      width: 180,
      sorter: (a: ConsumerGroupOffset, b: ConsumerGroupOffset) => a.lag - b.lag,
      render: (lag: number) => {
        const { color, status } = getLagStatus(lag);
        return (
          <Space>
            {lag === 0 ? (
              <CheckCircleOutlined style={{ color }} />
            ) : (
              <WarningOutlined style={{ color }} />
            )}
            <Text strong style={{ color }}>{lag?.toLocaleString()}</Text>
            <Progress
              percent={Math.min((lag / 10000) * 100, 100)}
              size="small"
              status={status}
              showInfo={false}
              style={{ width: 50 }}
            />
          </Space>
        );
      },
    },
    {
      title: 'Consumer',
      dataIndex: 'consumerId',
      key: 'consumerId',
      ellipsis: true,
      render: (id: string) => id || <Text type="secondary">-</Text>,
    },
    {
      title: 'Host',
      dataIndex: 'host',
      key: 'host',
      width: 150,
      render: (host: string) => host || <Text type="secondary">-</Text>,
    },
  ];

  if (!selectedClusterId) {
    return (
      <Card>
        <Empty description="Please select a cluster" />
      </Card>
    );
  }

  if (isLoading) {
    return (
      <div style={{ textAlign: 'center', padding: '100px 0' }}>
        <Spin size="large" />
      </div>
    );
  }

  const uniqueTopics: string[] = [...new Set(groupDetail?.offsets?.map((o: ConsumerGroupOffset) => o.topic) || [])];

  return (
    <div>
      <div style={{ display: 'flex', justifyContent: 'space-between', marginBottom: 16 }}>
        <Space>
          <Button icon={<ArrowLeftOutlined />} onClick={() => navigate('/consumer-groups')}>
            Back
          </Button>
          <Title level={4} style={{ margin: 0 }}>{decodedGroupId}</Title>
          <Tag color={getStateColor(groupDetail?.state)}>{groupDetail?.state || 'UNKNOWN'}</Tag>
        </Space>
        <Space>
          <Button icon={<ReloadOutlined />} onClick={() => refetch()}>
            Refresh
          </Button>
          <Button
            icon={<RollbackOutlined />}
            onClick={() => setResetModalOpen(true)}
            disabled={groupDetail?.state === 'STABLE'}
          >
            Reset Offsets
          </Button>
        </Space>
      </div>

      <Row gutter={[16, 16]} style={{ marginBottom: 24 }}>
        <Col xs={24} sm={8}>
          <Card>
            <Statistic title="Members" value={groupDetail?.members || 0} />
          </Card>
        </Col>
        <Col xs={24} sm={8}>
          <Card>
            <Statistic title="Topics" value={uniqueTopics.length} />
          </Card>
        </Col>
        <Col xs={24} sm={8}>
          <Card>
            <Statistic
              title="Total Lag"
              value={groupDetail?.totalLag || 0}
              valueStyle={{
                color: (groupDetail?.totalLag || 0) > 1000 ? '#ff4d4f' : '#52c41a',
              }}
            />
          </Card>
        </Col>
      </Row>

      <Card title="Group Details" style={{ marginBottom: 24 }}>
        <Descriptions bordered column={2}>
          <Descriptions.Item label="Group ID">{decodedGroupId}</Descriptions.Item>
          <Descriptions.Item label="State">
            <Tag color={getStateColor(groupDetail?.state)}>{groupDetail?.state}</Tag>
          </Descriptions.Item>
          <Descriptions.Item label="Coordinator">
            {groupDetail?.coordinator
              ? `${groupDetail.coordinator.host}:${groupDetail.coordinator.port} (ID: ${groupDetail.coordinator.id})`
              : '-'}
          </Descriptions.Item>
          <Descriptions.Item label="Partition Assignor">
            {groupDetail?.partitionAssignor || '-'}
          </Descriptions.Item>
        </Descriptions>
      </Card>

      <Card title="Partition Offsets">
        <Table
          columns={offsetColumns}
          dataSource={groupDetail?.offsets || []}
          rowKey={(record) => `${record.topic}-${record.partition}`}
          pagination={{
            showSizeChanger: true,
            showTotal: (total) => `Total ${total} partitions`,
          }}
          size="small"
        />
      </Card>

      {/* Reset Offsets Modal */}
      <Modal
        title="Reset Consumer Group Offsets"
        open={resetModalOpen}
        onCancel={() => {
          setResetModalOpen(false);
          resetForm.resetFields();
        }}
        footer={null}
      >
        <Form
          form={resetForm}
          layout="vertical"
          onFinish={(values) => resetOffsetsMutation.mutate(values)}
        >
          <Form.Item
            name="topic"
            label="Topic"
            rules={[{ required: true, message: 'Please select a topic' }]}
          >
            <Select
              placeholder="Select topic"
              options={uniqueTopics.map((t) => ({ value: t, label: t }))}
            />
          </Form.Item>

          <Form.Item
            name="strategy"
            label="Reset Strategy"
            rules={[{ required: true, message: 'Please select a strategy' }]}
          >
            <Select
              placeholder="Select strategy"
              options={[
                { value: 'EARLIEST', label: 'Earliest - Reset to beginning' },
                { value: 'LATEST', label: 'Latest - Reset to end' },
                { value: 'TO_OFFSET', label: 'To Offset - Reset to specific offset' },
                { value: 'SHIFT_BY', label: 'Shift By - Move offset by N' },
                { value: 'TO_DATETIME', label: 'To DateTime - Reset to timestamp' },
              ]}
            />
          </Form.Item>

          <Form.Item
            noStyle
            shouldUpdate={(prev, curr) => prev.strategy !== curr.strategy}
          >
            {({ getFieldValue }) => {
              const strategy = getFieldValue('strategy');
              if (['TO_OFFSET', 'SHIFT_BY', 'TO_DATETIME'].includes(strategy)) {
                return (
                  <Form.Item
                    name="value"
                    label={
                      strategy === 'TO_OFFSET'
                        ? 'Offset'
                        : strategy === 'SHIFT_BY'
                        ? 'Shift Amount'
                        : 'Timestamp (epoch ms)'
                    }
                    rules={[{ required: true, message: 'Please enter a value' }]}
                  >
                    <InputNumber style={{ width: '100%' }} />
                  </Form.Item>
                );
              }
              return null;
            }}
          </Form.Item>

          <Form.Item style={{ marginBottom: 0, textAlign: 'right' }}>
            <Space>
              <Button onClick={() => setResetModalOpen(false)}>Cancel</Button>
              <Button
                type="primary"
                htmlType="submit"
                loading={resetOffsetsMutation.isPending}
              >
                Reset Offsets
              </Button>
            </Space>
          </Form.Item>
        </Form>
      </Modal>
    </div>
  );
};

export default ConsumerGroupDetail;
