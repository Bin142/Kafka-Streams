import React from 'react';
import { Row, Col, Card, Statistic, Typography, Space, List, Tag, Progress, Alert } from 'antd';
import {
  DatabaseOutlined,
  TeamOutlined,
  MessageOutlined,
  ClusterOutlined,
  CheckCircleOutlined,
  WarningOutlined,
  DisconnectOutlined,
} from '@ant-design/icons';
import { useClusterStore } from '../store/clusterStore';

const { Title, Text } = Typography;

const Dashboard: React.FC = () => {
  const { selectedClusterId, getSelectedCluster } = useClusterStore();
  const cluster = getSelectedCluster();

  // Mock data - will be replaced with actual API calls when cluster is connected
  const stats = {
    topics: 24,
    consumerGroups: 12,
    messages: '1.2M',
    brokers: 3,
  };

  const recentTopics = [
    { name: 'orders', partitions: 6, messages: 125000 },
    { name: 'payments', partitions: 3, messages: 89000 },
    { name: 'notifications', partitions: 4, messages: 45000 },
    { name: 'user-events', partitions: 8, messages: 234000 },
  ];

  const consumerGroupsWithLag = [
    { name: 'order-processor', lag: 1250, status: 'warning' },
    { name: 'payment-handler', lag: 0, status: 'success' },
    { name: 'notification-sender', lag: 45, status: 'success' },
    { name: 'analytics-consumer', lag: 8900, status: 'error' },
  ];

  if (!selectedClusterId) {
    return (
      <div style={{ textAlign: 'center', padding: '100px 0' }}>
        <ClusterOutlined style={{ fontSize: 64, color: '#ccc' }} />
        <Title level={3} style={{ color: '#999', marginTop: 24 }}>
          Select a Cluster
        </Title>
        <Text type="secondary">
          Please select a Kafka cluster from the dropdown above to view the dashboard
        </Text>
      </div>
    );
  }

  const isConnected = cluster?.status === 'CONNECTED';

  return (
    <div>
      <Title level={4} style={{ marginBottom: 24 }}>
        Dashboard - {cluster?.name || selectedClusterId}
      </Title>

      {!isConnected && (
        <Alert
          message="Cluster Disconnected"
          description={
            <span>
              The Kafka cluster is not connected. Showing sample data. 
              To connect, make sure Kafka broker is running at <code>{cluster?.bootstrapServers}</code>
            </span>
          }
          type="warning"
          showIcon
          icon={<DisconnectOutlined />}
          style={{ marginBottom: 24 }}
        />
      )}

      {/* Stats Cards */}
      <Row gutter={[16, 16]}>
        <Col xs={24} sm={12} lg={6}>
          <Card hoverable>
            <Statistic
              title="Topics"
              value={stats.topics}
              prefix={<DatabaseOutlined style={{ color: '#1890ff' }} />}
              valueStyle={{ color: isConnected ? undefined : '#999' }}
            />
          </Card>
        </Col>
        <Col xs={24} sm={12} lg={6}>
          <Card hoverable>
            <Statistic
              title="Consumer Groups"
              value={stats.consumerGroups}
              prefix={<TeamOutlined style={{ color: '#52c41a' }} />}
              valueStyle={{ color: isConnected ? undefined : '#999' }}
            />
          </Card>
        </Col>
        <Col xs={24} sm={12} lg={6}>
          <Card hoverable>
            <Statistic
              title="Messages (24h)"
              value={stats.messages}
              prefix={<MessageOutlined style={{ color: '#722ed1' }} />}
              valueStyle={{ color: isConnected ? undefined : '#999' }}
            />
          </Card>
        </Col>
        <Col xs={24} sm={12} lg={6}>
          <Card hoverable>
            <Statistic
              title="Brokers"
              value={isConnected ? stats.brokers : 0}
              prefix={<ClusterOutlined style={{ color: '#fa8c16' }} />}
              suffix={
                <Tag color={isConnected ? 'success' : 'error'} style={{ marginLeft: 8 }}>
                  {isConnected ? 'Healthy' : 'Offline'}
                </Tag>
              }
            />
          </Card>
        </Col>
      </Row>

      {/* Recent Topics & Consumer Groups */}
      <Row gutter={[16, 16]} style={{ marginTop: 24 }}>
        <Col xs={24} lg={12}>
          <Card title="Top Topics by Messages" extra={<a href="/topics">View All</a>}>
            <List
              dataSource={recentTopics}
              renderItem={(item) => (
                <List.Item>
                  <List.Item.Meta
                    avatar={<DatabaseOutlined style={{ fontSize: 20, color: '#1890ff' }} />}
                    title={item.name}
                    description={`${item.partitions} partitions`}
                  />
                  <Text strong style={{ color: isConnected ? undefined : '#999' }}>
                    {item.messages.toLocaleString()} msgs
                  </Text>
                </List.Item>
              )}
            />
          </Card>
        </Col>

        <Col xs={24} lg={12}>
          <Card title="Consumer Group Lag" extra={<a href="/consumer-groups">View All</a>}>
            <List
              dataSource={consumerGroupsWithLag}
              renderItem={(item) => (
                <List.Item>
                  <List.Item.Meta
                    avatar={
                      item.status === 'success' ? (
                        <CheckCircleOutlined style={{ fontSize: 20, color: '#52c41a' }} />
                      ) : (
                        <WarningOutlined
                          style={{
                            fontSize: 20,
                            color: item.status === 'error' ? '#ff4d4f' : '#faad14',
                          }}
                        />
                      )
                    }
                    title={item.name}
                    description={
                      <Progress
                        percent={Math.min((item.lag / 10000) * 100, 100)}
                        size="small"
                        status={
                          item.status === 'error'
                            ? 'exception'
                            : item.status === 'warning'
                            ? 'active'
                            : 'success'
                        }
                        showInfo={false}
                      />
                    }
                  />
                  <Text
                    type={item.status === 'error' ? 'danger' : item.status === 'warning' ? 'warning' : undefined}
                    strong
                    style={{ color: isConnected ? undefined : '#999' }}
                  >
                    {item.lag.toLocaleString()} lag
                  </Text>
                </List.Item>
              )}
            />
          </Card>
        </Col>
      </Row>

      {/* Cluster Health */}
      <Row gutter={[16, 16]} style={{ marginTop: 24 }}>
        <Col span={24}>
          <Card title="Cluster Health">
            <Row gutter={[32, 16]}>
              <Col xs={24} sm={8}>
                <Space direction="vertical" style={{ width: '100%' }}>
                  <Text type="secondary">Under Replicated Partitions</Text>
                  <Statistic 
                    value={isConnected ? 0 : '-'} 
                    valueStyle={{ color: isConnected ? '#52c41a' : '#999' }} 
                  />
                </Space>
              </Col>
              <Col xs={24} sm={8}>
                <Space direction="vertical" style={{ width: '100%' }}>
                  <Text type="secondary">Offline Partitions</Text>
                  <Statistic 
                    value={isConnected ? 0 : '-'} 
                    valueStyle={{ color: isConnected ? '#52c41a' : '#999' }} 
                  />
                </Space>
              </Col>
              <Col xs={24} sm={8}>
                <Space direction="vertical" style={{ width: '100%' }}>
                  <Text type="secondary">Active Controllers</Text>
                  <Statistic 
                    value={isConnected ? 1 : 0} 
                    valueStyle={{ color: isConnected ? '#52c41a' : '#ff4d4f' }} 
                  />
                </Space>
              </Col>
            </Row>
          </Card>
        </Col>
      </Row>
    </div>
  );
};

export default Dashboard;
