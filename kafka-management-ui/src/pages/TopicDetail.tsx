import React, { useState, useEffect } from 'react';
import { useParams, useNavigate } from 'react-router-dom';
import {
  Tabs,
  Card,
  Descriptions,
  Table,
  Button,
  Space,
  Tag,
  Typography,
  Spin,
  Input,
  Select,
  Form,
  Modal,
  InputNumber,
  message,
  Tooltip,
  Empty,
  Drawer,
  Alert,
  Badge,
} from 'antd';
import {
  ArrowLeftOutlined,
  ReloadOutlined,
  SendOutlined,
  SettingOutlined,
  CopyOutlined,
  DisconnectOutlined,
  ApartmentOutlined,
  TeamOutlined,
} from '@ant-design/icons';
import { useQuery, useMutation } from '@tanstack/react-query';
import { useClusterStore } from '../store/clusterStore';
import { topicApi, messageApi, consumerGroupApi } from '../api';
import { mockTopics, mockMessages, mockConsumerGroups } from '../api/mockData';
import type { TopicConfig, Message, Partition, TopicDetail as TopicDetailType, ConsumerGroup } from '../api/types';
import dayjs from 'dayjs';
import mermaid from 'mermaid';

// Initialize mermaid
mermaid.initialize({
  startOnLoad: false,
  theme: 'default',
  securityLevel: 'loose',
  flowchart: {
    useMaxWidth: true,
    htmlLabels: true,
    curve: 'basis',
  },
});

const { Title, Text, Paragraph } = Typography;
const { TextArea } = Input;

// Mock producer data
const mockProducers: Record<string, string[]> = {
  'orders': ['order-service', 'checkout-service'],
  'payments': ['payment-service'],
  'notifications': ['notification-service', 'email-service'],
  'user-events': ['user-service', 'auth-service'],
  'inventory': ['inventory-service'],
  'shipping': ['shipping-service', 'logistics-service'],
  'analytics': ['analytics-collector'],
};

// Mermaid Diagram Component
interface MermaidDiagramProps {
  chart: string;
  id: string;
}

const MermaidDiagram: React.FC<MermaidDiagramProps> = ({ chart, id }) => {
  const [svg, setSvg] = useState<string>('');

  useEffect(() => {
    const renderDiagram = async () => {
      try {
        const existingElement = document.getElementById(id);
        if (existingElement) {
          existingElement.remove();
        }
        const { svg } = await mermaid.render(id, chart);
        setSvg(svg);
      } catch (error) {
        console.error('Mermaid render error:', error);
      }
    };
    if (chart.trim()) {
      renderDiagram();
    }
  }, [chart, id]);

  return (
    <div 
      dangerouslySetInnerHTML={{ __html: svg }}
      style={{ 
        display: 'flex', 
        justifyContent: 'center', 
        padding: '20px',
        overflow: 'auto',
        background: '#fafafa',
        borderRadius: '8px',
        minHeight: 200,
      }}
    />
  );
};

const TopicDetail: React.FC = () => {
  const { topicName } = useParams<{ topicName: string }>();
  const navigate = useNavigate();
  const { selectedClusterId, getSelectedCluster } = useClusterStore();
  const cluster = getSelectedCluster();
  const decodedTopicName = decodeURIComponent(topicName || '');
  const isConnected = cluster?.status === 'CONNECTED';

  const [activeTab, setActiveTab] = useState('overview');
  const [produceModalOpen, setProduceModalOpen] = useState(false);
  const [configDrawerOpen, setConfigDrawerOpen] = useState(false);
  const [messageDrawerOpen, setMessageDrawerOpen] = useState(false);
  const [selectedMessage, setSelectedMessage] = useState<Message | null>(null);
  const [browseParams, setBrowseParams] = useState({
    partition: undefined as number | undefined,
    offset: undefined as number | undefined,
    limit: 50,
  });
  const [produceForm] = Form.useForm();

  const { data: topicDetail, isLoading: topicLoading, error: topicError } = useQuery({
    queryKey: ['topic', selectedClusterId, decodedTopicName],
    queryFn: () => topicApi.getTopic(selectedClusterId!, decodedTopicName),
    enabled: !!selectedClusterId && !!decodedTopicName && isConnected,
    retry: false,
  });

  const { data: topicConfigs, isLoading: configsLoading } = useQuery({
    queryKey: ['topicConfigs', selectedClusterId, decodedTopicName],
    queryFn: () => topicApi.getTopicConfigs(selectedClusterId!, decodedTopicName),
    enabled: !!selectedClusterId && !!decodedTopicName && isConnected,
    retry: false,
  });

  const { data: messagesData, isLoading: messagesLoading, refetch: refetchMessages } = useQuery({
    queryKey: ['messages', selectedClusterId, decodedTopicName, browseParams],
    queryFn: () => messageApi.browseMessages(selectedClusterId!, decodedTopicName, browseParams),
    enabled: !!selectedClusterId && !!decodedTopicName && activeTab === 'messages' && isConnected,
    retry: false,
  });

  // Fetch consumer groups for this topic
  const { data: consumerGroups } = useQuery({
    queryKey: ['consumerGroups', selectedClusterId],
    queryFn: () => consumerGroupApi.listConsumerGroups(selectedClusterId!),
    enabled: !!selectedClusterId && isConnected,
    retry: false,
    staleTime: 30000,
  });

  // Use mock data when disconnected
  const displayGroups = isConnected ? (consumerGroups || []) : mockConsumerGroups;
  
  // Filter consumer groups that listen to this topic
  const topicConsumerGroups = displayGroups.filter((g: ConsumerGroup) => 
    g.topics.includes(decodedTopicName)
  );

  // Get producers for this topic
  const topicProducers = mockProducers[decodedTopicName] || [`${decodedTopicName}-producer`];

  // Build mermaid flow diagram for this topic
  const buildTopicFlowDiagram = () => {
    const lines: string[] = ['flowchart LR'];
    
    // Producers
    lines.push('  subgraph Producers["🚀 Producers"]');
    topicProducers.forEach(producer => {
      const safeName = producer.replace(/-/g, '_');
      lines.push(`    ${safeName}["${producer}"]`);
    });
    lines.push('  end');
    
    // Topic
    const topicSafe = decodedTopicName.replace(/-/g, '_');
    lines.push(`  ${topicSafe}[["📨 ${decodedTopicName}<br/>Partitions: ${displayTopicDetail?.partitionCount || '?'}"]]`);
    
    // Consumer Groups
    if (topicConsumerGroups.length > 0) {
      lines.push('  subgraph Consumers["👥 Consumer Groups"]');
      topicConsumerGroups.forEach((group: ConsumerGroup) => {
        const safeName = group.groupId.replace(/-/g, '_');
        const stateIcon = group.state === 'STABLE' ? '✅' : group.state === 'EMPTY' ? '⚪' : '🔄';
        lines.push(`    ${safeName}("${stateIcon} ${group.groupId}<br/>Members: ${group.members} | Lag: ${group.totalLag}")`);
      });
      lines.push('  end');
    } else {
      lines.push('  no_consumers(("No Consumers"))');
    }
    
    // Connections: Producers -> Topic
    topicProducers.forEach(producer => {
      const producerSafe = producer.replace(/-/g, '_');
      lines.push(`  ${producerSafe} --> ${topicSafe}`);
    });
    
    // Connections: Topic -> Consumer Groups
    if (topicConsumerGroups.length > 0) {
      topicConsumerGroups.forEach((group: ConsumerGroup) => {
        const groupSafe = group.groupId.replace(/-/g, '_');
        lines.push(`  ${topicSafe} --> ${groupSafe}`);
      });
    } else {
      lines.push(`  ${topicSafe} -.-> no_consumers`);
    }
    
    // Styling
    lines.push('  style Producers fill:#e6f7ff,stroke:#1890ff');
    lines.push(`  style ${topicSafe} fill:#fff7e6,stroke:#fa8c16`);
    if (topicConsumerGroups.length > 0) {
      lines.push('  style Consumers fill:#f6ffed,stroke:#52c41a');
    }
    
    return lines.join('\n');
  };

  // Mock data for disconnected state
  const mockTopic = mockTopics.find(t => t.name === decodedTopicName);
  const mockTopicDetail: TopicDetailType | undefined = mockTopic ? {
    ...mockTopic,
    partitions: Array.from({ length: mockTopic.partitionCount }, (_, i) => ({
      partition: i,
      leader: i % 3,
      replicas: [0, 1, 2],
      isr: [0, 1, 2],
      beginningOffset: 0,
      endOffset: Math.floor(mockTopic.messageCount / mockTopic.partitionCount),
      messageCount: Math.floor(mockTopic.messageCount / mockTopic.partitionCount),
    })),
  } : undefined;

  const mockTopicMessages = mockMessages[decodedTopicName] || [];

  // Use mock data when disconnected
  const displayTopicDetail = isConnected && !topicError ? topicDetail : mockTopicDetail;
  const displayMessages = isConnected && !topicError ? (messagesData?.messages || []) : mockTopicMessages;

  // Filter messages by partition if selected
  const filteredMessages = browseParams.partition !== undefined 
    ? displayMessages.filter((m: Message) => m.partition === browseParams.partition)
    : displayMessages;

  const produceMutation = useMutation({
    mutationFn: (data: { key?: string; value: string; headers?: Record<string, string>; partition?: number }) =>
      messageApi.produceMessage(selectedClusterId!, decodedTopicName, data),
    onSuccess: () => {
      message.success('Message produced successfully');
      setProduceModalOpen(false);
      produceForm.resetFields();
      refetchMessages();
    },
    onError: (error: Error) => {
      message.error(`Failed to produce message: ${error.message}`);
    },
  });

  const partitionColumns = [
    { title: 'Partition', dataIndex: 'partition', key: 'partition', width: 100 },
    { title: 'Leader', dataIndex: 'leader', key: 'leader', width: 100 },
    {
      title: 'Replicas',
      dataIndex: 'replicas',
      key: 'replicas',
      render: (replicas: number[]) => replicas?.join(', ') || '-',
    },
    {
      title: 'ISR',
      dataIndex: 'isr',
      key: 'isr',
      render: (isr: number[]) => isr?.join(', ') || '-',
    },
    { title: 'Begin Offset', dataIndex: 'beginningOffset', key: 'beginningOffset' },
    { title: 'End Offset', dataIndex: 'endOffset', key: 'endOffset' },
    {
      title: 'Messages',
      dataIndex: 'messageCount',
      key: 'messageCount',
      render: (count: number) => count?.toLocaleString() || '-',
    },
  ];

  const configColumns = [
    { title: 'Name', dataIndex: 'name', key: 'name', width: '30%' },
    {
      title: 'Value',
      dataIndex: 'value',
      key: 'value',
      width: '30%',
      render: (value: string, record: TopicConfig) => (
        <Text code={!record.isSensitive}>
          {record.isSensitive ? '********' : value || '-'}
        </Text>
      ),
    },
    {
      title: 'Source',
      dataIndex: 'source',
      key: 'source',
      width: '20%',
      render: (source: string) => (
        <Tag color={source === 'DEFAULT_CONFIG' ? 'default' : 'blue'}>{source}</Tag>
      ),
    },
    {
      title: 'Default',
      dataIndex: 'isDefault',
      key: 'isDefault',
      width: '10%',
      render: (isDefault: boolean) => (isDefault ? <Tag>Default</Tag> : null),
    },
  ];

  const messageColumns = [
    { title: 'Partition', dataIndex: 'partition', key: 'partition', width: 90 },
    { title: 'Offset', dataIndex: 'offset', key: 'offset', width: 100 },
    {
      title: 'Timestamp',
      dataIndex: 'timestamp',
      key: 'timestamp',
      width: 180,
      render: (ts: string) => dayjs(ts).format('YYYY-MM-DD HH:mm:ss'),
    },
    {
      title: 'Key',
      dataIndex: 'key',
      key: 'key',
      width: 150,
      ellipsis: true,
      render: (key: string | null) => (
        <Text code style={{ maxWidth: 130 }} ellipsis>
          {key || <Text type="secondary">null</Text>}
        </Text>
      ),
    },
    {
      title: 'Value',
      dataIndex: 'value',
      key: 'value',
      ellipsis: true,
      render: (value: string | null) => (
        <Paragraph
          ellipsis={{ rows: 1 }}
          style={{ marginBottom: 0, maxWidth: 300 }}
        >
          {value || <Text type="secondary">null</Text>}
        </Paragraph>
      ),
    },
    {
      title: 'Actions',
      key: 'actions',
      width: 100,
      render: (_: unknown, record: Message) => (
        <Space>
          <Tooltip title="View Details">
            <Button
              type="text"
              size="small"
              onClick={() => {
                setSelectedMessage(record);
                setMessageDrawerOpen(true);
              }}
            >
              View
            </Button>
          </Tooltip>
          <Tooltip title="Copy Value">
            <Button
              type="text"
              size="small"
              icon={<CopyOutlined />}
              onClick={() => {
                navigator.clipboard.writeText(record.value || '');
                message.success('Copied to clipboard');
              }}
            />
          </Tooltip>
        </Space>
      ),
    },
  ];

  if (!selectedClusterId) {
    return (
      <Card>
        <Empty description="Please select a cluster" />
      </Card>
    );
  }

  if (topicLoading && isConnected) {
    return (
      <div style={{ textAlign: 'center', padding: '100px 0' }}>
        <Spin size="large" />
      </div>
    );
  }

  if (!displayTopicDetail) {
    return (
      <Card>
        <Empty description={`Topic "${decodedTopicName}" not found`} />
      </Card>
    );
  }

  const tabItems = [
    {
      key: 'overview',
      label: 'Overview',
      children: (
        <div>
          {!isConnected && (
            <Alert
              message="Cluster Disconnected - Showing Sample Data"
              type="warning"
              showIcon
              icon={<DisconnectOutlined />}
              style={{ marginBottom: 16 }}
            />
          )}
          <Descriptions bordered column={2} style={{ marginBottom: 24 }}>
            <Descriptions.Item label="Topic Name">{decodedTopicName}</Descriptions.Item>
            <Descriptions.Item label="Internal">
              {displayTopicDetail?.internal ? <Tag color="warning">Yes</Tag> : <Tag>No</Tag>}
            </Descriptions.Item>
            <Descriptions.Item label="Partitions">{displayTopicDetail?.partitionCount}</Descriptions.Item>
            <Descriptions.Item label="Replication Factor">{displayTopicDetail?.replicationFactor}</Descriptions.Item>
            <Descriptions.Item label="Total Messages" span={2}>
              {displayTopicDetail?.messageCount?.toLocaleString() || '-'}
            </Descriptions.Item>
          </Descriptions>

          <Title level={5}>Partitions</Title>
          <Table
            columns={partitionColumns}
            dataSource={displayTopicDetail?.partitions || []}
            rowKey="partition"
            pagination={false}
            size="small"
          />
        </div>
      ),
    },
    {
      key: 'flow',
      label: (
        <span>
          <ApartmentOutlined /> Message Flow
        </span>
      ),
      children: (
        <div>
          {!isConnected && (
            <Alert
              message="Cluster Disconnected - Showing Sample Flow"
              type="warning"
              showIcon
              icon={<DisconnectOutlined />}
              style={{ marginBottom: 16 }}
            />
          )}
          <Card size="small" style={{ marginBottom: 16 }}>
            <Space size="large">
              <span><Tag color="blue">🚀 Producers</Tag> Services producing to this topic</span>
              <span><Tag color="orange">📨 Topic</Tag> Current topic</span>
              <span><Tag color="green">👥 Consumers</Tag> ✅ Stable | ⚪ Empty | 🔄 Rebalancing</span>
            </Space>
          </Card>
          <MermaidDiagram chart={buildTopicFlowDiagram()} id={`topic-flow-${decodedTopicName.replace(/[^a-zA-Z0-9]/g, '_')}`} />
          
          {/* Consumer Groups Table */}
          <Title level={5} style={{ marginTop: 24 }}>
            <TeamOutlined /> Consumer Groups ({topicConsumerGroups.length})
          </Title>
          {topicConsumerGroups.length > 0 ? (
            <Table
              dataSource={topicConsumerGroups}
              rowKey="groupId"
              size="small"
              pagination={false}
              columns={[
                {
                  title: 'Group ID',
                  dataIndex: 'groupId',
                  key: 'groupId',
                  render: (groupId: string) => (
                    <a onClick={() => navigate(`/consumer-groups/${encodeURIComponent(groupId)}`)}>{groupId}</a>
                  ),
                },
                {
                  title: 'State',
                  dataIndex: 'state',
                  key: 'state',
                  width: 120,
                  render: (state: string) => {
                    const color = state === 'STABLE' ? 'success' : state === 'EMPTY' ? 'default' : 'processing';
                    return <Badge status={color} text={state} />;
                  },
                },
                {
                  title: 'Members',
                  dataIndex: 'members',
                  key: 'members',
                  width: 100,
                },
                {
                  title: 'Total Lag',
                  dataIndex: 'totalLag',
                  key: 'totalLag',
                  width: 120,
                  render: (lag: number) => (
                    <span style={{ color: lag > 1000 ? '#ff4d4f' : lag > 0 ? '#faad14' : '#52c41a' }}>
                      {lag.toLocaleString()}
                    </span>
                  ),
                },
              ]}
            />
          ) : (
            <Empty description="No consumer groups are listening to this topic" />
          )}
        </div>
      ),
    },
    {
      key: 'messages',
      label: 'Messages',
      children: (
        <div>
          {!isConnected && (
            <Alert
              message="Cluster Disconnected - Showing Sample Messages"
              description="Connect to Kafka to browse real messages and produce new ones."
              type="warning"
              showIcon
              icon={<DisconnectOutlined />}
              style={{ marginBottom: 16 }}
            />
          )}
          <Space style={{ marginBottom: 16 }} wrap>
            <Select
              placeholder="All Partitions"
              allowClear
              style={{ width: 150 }}
              value={browseParams.partition}
              onChange={(value) => setBrowseParams({ ...browseParams, partition: value })}
              options={displayTopicDetail?.partitions?.map((p: Partition) => ({
                value: p.partition,
                label: `Partition ${p.partition}`,
              }))}
            />
            <InputNumber
              placeholder="Start Offset"
              style={{ width: 150 }}
              value={browseParams.offset}
              onChange={(value) => setBrowseParams({ ...browseParams, offset: value || undefined })}
              disabled={!isConnected}
            />
            <Select
              value={browseParams.limit}
              onChange={(value) => setBrowseParams({ ...browseParams, limit: value })}
              style={{ width: 120 }}
              options={[
                { value: 20, label: '20 messages' },
                { value: 50, label: '50 messages' },
                { value: 100, label: '100 messages' },
                { value: 200, label: '200 messages' },
              ]}
            />
            <Button icon={<ReloadOutlined />} onClick={() => refetchMessages()} disabled={!isConnected}>
              Refresh
            </Button>
            <Button
              type="primary"
              icon={<SendOutlined />}
              onClick={() => setProduceModalOpen(true)}
              disabled={!isConnected}
            >
              Produce Message
            </Button>
          </Space>

          <Table
            columns={messageColumns}
            dataSource={filteredMessages}
            rowKey={(record) => `${record.partition}-${record.offset}`}
            loading={messagesLoading && isConnected}
            pagination={{
              showSizeChanger: false,
              showTotal: (total) => `${total} messages`,
            }}
            size="small"
          />
        </div>
      ),
    },
    {
      key: 'configs',
      label: 'Configuration',
      children: (
        <div>
          <div style={{ marginBottom: 16, textAlign: 'right' }}>
            <Button
              icon={<SettingOutlined />}
              onClick={() => setConfigDrawerOpen(true)}
            >
              Edit Configuration
            </Button>
          </div>
          <Table
            columns={configColumns}
            dataSource={topicConfigs || []}
            rowKey="name"
            loading={configsLoading}
            pagination={false}
            size="small"
          />
        </div>
      ),
    },
  ];

  return (
    <div>
      <div style={{ display: 'flex', justifyContent: 'space-between', marginBottom: 16 }}>
        <Space>
          <Button icon={<ArrowLeftOutlined />} onClick={() => navigate('/topics')}>
            Back
          </Button>
          <Title level={4} style={{ margin: 0 }}>{decodedTopicName}</Title>
          {displayTopicDetail?.internal && <Tag color="warning">Internal</Tag>}
        </Space>
      </div>

      <Card>
        <Tabs activeKey={activeTab} onChange={setActiveTab} items={tabItems} />
      </Card>

      {/* Produce Message Modal */}
      <Modal
        title="Produce Message"
        open={produceModalOpen}
        onCancel={() => {
          setProduceModalOpen(false);
          produceForm.resetFields();
        }}
        footer={null}
        width={600}
      >
        <Form
          form={produceForm}
          layout="vertical"
          onFinish={(values) => produceMutation.mutate(values)}
        >
          <Form.Item name="partition" label="Partition (optional)">
            <Select
              placeholder="Auto (Round Robin)"
              allowClear
              options={topicDetail?.partitions?.map((p: Partition) => ({
                value: p.partition,
                label: `Partition ${p.partition}`,
              }))}
            />
          </Form.Item>
          <Form.Item name="key" label="Key (optional)">
            <Input placeholder="Message key" />
          </Form.Item>
          <Form.Item
            name="value"
            label="Value"
            rules={[{ required: true, message: 'Please enter message value' }]}
          >
            <TextArea rows={6} placeholder='{"example": "json value"}' />
          </Form.Item>
          <Form.Item style={{ marginBottom: 0, textAlign: 'right' }}>
            <Space>
              <Button onClick={() => setProduceModalOpen(false)}>Cancel</Button>
              <Button
                type="primary"
                htmlType="submit"
                icon={<SendOutlined />}
                loading={produceMutation.isPending}
              >
                Produce
              </Button>
            </Space>
          </Form.Item>
        </Form>
      </Modal>

      {/* Message Detail Drawer */}
      <Drawer
        title={`Message - Partition ${selectedMessage?.partition}, Offset ${selectedMessage?.offset}`}
        open={messageDrawerOpen}
        onClose={() => setMessageDrawerOpen(false)}
        width={600}
      >
        {selectedMessage && (
          <div>
            <Descriptions bordered column={1} size="small" style={{ marginBottom: 16 }}>
              <Descriptions.Item label="Partition">{selectedMessage.partition}</Descriptions.Item>
              <Descriptions.Item label="Offset">{selectedMessage.offset}</Descriptions.Item>
              <Descriptions.Item label="Timestamp">
                {dayjs(selectedMessage.timestamp).format('YYYY-MM-DD HH:mm:ss.SSS')}
              </Descriptions.Item>
              <Descriptions.Item label="Key Format">{selectedMessage.keyFormat}</Descriptions.Item>
              <Descriptions.Item label="Value Format">{selectedMessage.valueFormat}</Descriptions.Item>
            </Descriptions>

            <Title level={5}>Key</Title>
            <Card size="small" style={{ marginBottom: 16 }}>
              <pre style={{ margin: 0, whiteSpace: 'pre-wrap', wordBreak: 'break-all' }}>
                {selectedMessage.key || 'null'}
              </pre>
            </Card>

            <Title level={5}>Value</Title>
            <Card size="small" style={{ marginBottom: 16 }}>
              <pre style={{ margin: 0, whiteSpace: 'pre-wrap', wordBreak: 'break-all' }}>
                {(() => {
                  try {
                    return JSON.stringify(JSON.parse(selectedMessage.value || ''), null, 2);
                  } catch {
                    return selectedMessage.value || 'null';
                  }
                })()}
              </pre>
            </Card>

            {Object.keys(selectedMessage.headers || {}).length > 0 && (
              <>
                <Title level={5}>Headers</Title>
                <Card size="small">
                  <pre style={{ margin: 0 }}>
                    {JSON.stringify(selectedMessage.headers, null, 2)}
                  </pre>
                </Card>
              </>
            )}
          </div>
        )}
      </Drawer>

      {/* Config Edit Drawer */}
      <Drawer
        title="Edit Topic Configuration"
        open={configDrawerOpen}
        onClose={() => setConfigDrawerOpen(false)}
        width={500}
      >
        <Text type="secondary" style={{ display: 'block', marginBottom: 16 }}>
          Modify topic configuration values. Only non-default, non-readonly configs can be changed.
        </Text>
        {/* Config editing form would go here */}
        <Empty description="Configuration editor coming soon" />
      </Drawer>
    </div>
  );
};

export default TopicDetail;
