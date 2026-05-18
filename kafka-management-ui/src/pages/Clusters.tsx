import React, { useState } from 'react';
import {
  Table,
  Button,
  Input,
  Space,
  Tag,
  Typography,
  Modal,
  Form,
  message,
  Tooltip,
  Popconfirm,
  Descriptions,
  Badge,
  Drawer,
  List,
} from 'antd';
import {
  PlusOutlined,
  SearchOutlined,
  ReloadOutlined,
  DeleteOutlined,
  EyeOutlined,
  ClusterOutlined,
  CheckCircleOutlined,
} from '@ant-design/icons';
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query';
import { clusterApi } from '../api';
import type { Cluster, ClusterDetail, Node } from '../api/types';

const { Title, Text } = Typography;

const Clusters: React.FC = () => {
  const queryClient = useQueryClient();
  const [searchText, setSearchText] = useState('');
  const [modalOpen, setModalOpen] = useState(false);
  const [detailDrawerOpen, setDetailDrawerOpen] = useState(false);
  const [selectedCluster, setSelectedCluster] = useState<ClusterDetail | null>(null);
  const [form] = Form.useForm();

  const { data: clusters, isLoading, refetch } = useQuery({
    queryKey: ['clusters'],
    queryFn: () => clusterApi.listClusters(),
  });

  const createMutation = useMutation({
    mutationFn: (data: Partial<Cluster>) => clusterApi.createCluster(data),
    onSuccess: () => {
      message.success('Cluster added successfully');
      closeModal();
      queryClient.invalidateQueries({ queryKey: ['clusters'] });
    },
    onError: (error: Error) => {
      message.error(`Failed to add cluster: ${error.message}`);
    },
  });

  const deleteMutation = useMutation({
    mutationFn: (id: string) => clusterApi.deleteCluster(id),
    onSuccess: () => {
      message.success('Cluster removed successfully');
      queryClient.invalidateQueries({ queryKey: ['clusters'] });
    },
    onError: (error: Error) => {
      message.error(`Failed to remove cluster: ${error.message}`);
    },
  });

  const closeModal = () => {
    setModalOpen(false);
    form.resetFields();
  };

  const openDetailDrawer = async (cluster: Cluster) => {
    try {
      const detail = await clusterApi.getCluster(cluster.id);
      setSelectedCluster(detail);
      setDetailDrawerOpen(true);
    } catch (error) {
      message.error('Failed to load cluster details');
    }
  };

  const handleSubmit = (values: Record<string, unknown>) => {
    createMutation.mutate(values as Partial<Cluster>);
  };

  const filteredClusters = clusters?.filter((cluster: Cluster) =>
    cluster.name.toLowerCase().includes(searchText.toLowerCase()) ||
    cluster.bootstrapServers.toLowerCase().includes(searchText.toLowerCase())
  ) || [];

  const columns = [
    {
      title: 'Cluster Name',
      dataIndex: 'name',
      key: 'name',
      sorter: (a: Cluster, b: Cluster) => a.name.localeCompare(b.name),
      render: (name: string, record: Cluster) => (
        <Space>
          <ClusterOutlined style={{ color: '#1890ff' }} />
          <a onClick={() => openDetailDrawer(record)}>{name}</a>
        </Space>
      ),
    },
    {
      title: 'Bootstrap Servers',
      dataIndex: 'bootstrapServers',
      key: 'bootstrapServers',
      ellipsis: true,
      render: (servers: string) => <Text code>{servers}</Text>,
    },
    {
      title: 'Status',
      dataIndex: 'status',
      key: 'status',
      width: 130,
      filters: [
        { text: 'Connected', value: 'CONNECTED' },
        { text: 'Disconnected', value: 'DISCONNECTED' },
        { text: 'Error', value: 'ERROR' },
      ],
      onFilter: (value: unknown, record: Cluster) => record.status === value,
      render: (status: string) => (
        <Badge
          status={status === 'CONNECTED' ? 'success' : status === 'ERROR' ? 'error' : 'default'}
          text={status}
        />
      ),
    },
    {
      title: 'Nodes',
      dataIndex: 'nodeCount',
      key: 'nodeCount',
      width: 100,
    },
    {
      title: 'Actions',
      key: 'actions',
      width: 150,
      render: (_: unknown, record: Cluster) => (
        <Space>
          <Tooltip title="View Details">
            <Button
              type="text"
              icon={<EyeOutlined />}
              onClick={() => openDetailDrawer(record)}
            />
          </Tooltip>
          <Popconfirm
            title="Remove Cluster"
            description={`Are you sure you want to remove "${record.name}"?`}
            onConfirm={() => deleteMutation.mutate(record.id)}
            okText="Yes"
            cancelText="No"
            okButtonProps={{ danger: true }}
          >
            <Tooltip title="Remove">
              <Button type="text" danger icon={<DeleteOutlined />} />
            </Tooltip>
          </Popconfirm>
        </Space>
      ),
    },
  ];

  return (
    <div>
      <div style={{ display: 'flex', justifyContent: 'space-between', marginBottom: 16 }}>
        <Title level={4} style={{ margin: 0 }}>Clusters</Title>
        <Space>
          <Input
            placeholder="Search clusters..."
            prefix={<SearchOutlined />}
            value={searchText}
            onChange={(e) => setSearchText(e.target.value)}
            style={{ width: 250 }}
            allowClear
          />
          <Button icon={<ReloadOutlined />} onClick={() => refetch()}>
            Refresh
          </Button>
          <Button
            type="primary"
            icon={<PlusOutlined />}
            onClick={() => setModalOpen(true)}
          >
            Add Cluster
          </Button>
        </Space>
      </div>

      <Table
        columns={columns}
        dataSource={filteredClusters}
        rowKey="id"
        loading={isLoading}
        pagination={{
          showSizeChanger: true,
          showQuickJumper: true,
          showTotal: (total) => `Total ${total} clusters`,
        }}
      />

      {/* Add Cluster Modal */}
      <Modal
        title="Add New Cluster"
        open={modalOpen}
        onCancel={closeModal}
        footer={null}
        width={500}
      >
        <Form form={form} layout="vertical" onFinish={handleSubmit}>
          <Form.Item
            name="name"
            label="Cluster Name"
            rules={[{ required: true, message: 'Please enter cluster name' }]}
          >
            <Input placeholder="Production Cluster" />
          </Form.Item>

          <Form.Item
            name="bootstrapServers"
            label="Bootstrap Servers"
            rules={[{ required: true, message: 'Please enter bootstrap servers' }]}
          >
            <Input placeholder="localhost:9092,localhost:9093" />
          </Form.Item>

          <Form.Item style={{ marginBottom: 0, textAlign: 'right' }}>
            <Space>
              <Button onClick={closeModal}>Cancel</Button>
              <Button type="primary" htmlType="submit" loading={createMutation.isPending}>
                Add Cluster
              </Button>
            </Space>
          </Form.Item>
        </Form>
      </Modal>

      {/* Cluster Detail Drawer */}
      <Drawer
        title={`Cluster: ${selectedCluster?.name}`}
        open={detailDrawerOpen}
        onClose={() => {
          setDetailDrawerOpen(false);
          setSelectedCluster(null);
        }}
        width={600}
      >
        {selectedCluster && (
          <div>
            <Descriptions bordered column={1} style={{ marginBottom: 24 }}>
              <Descriptions.Item label="Name">{selectedCluster.name}</Descriptions.Item>
              <Descriptions.Item label="Cluster ID">{selectedCluster.clusterId}</Descriptions.Item>
              <Descriptions.Item label="Bootstrap Servers">
                <Text code>{selectedCluster.bootstrapServers}</Text>
              </Descriptions.Item>
              <Descriptions.Item label="Status">
                <Badge
                  status={selectedCluster.status === 'CONNECTED' ? 'success' : 'error'}
                  text={selectedCluster.status}
                />
              </Descriptions.Item>
              <Descriptions.Item label="Controller">
                {selectedCluster.controller
                  ? `${selectedCluster.controller.host}:${selectedCluster.controller.port} (ID: ${selectedCluster.controller.id})`
                  : '-'}
              </Descriptions.Item>
            </Descriptions>

            <Title level={5}>Broker Nodes ({selectedCluster.nodes?.length || 0})</Title>
            <List
              dataSource={selectedCluster.nodes || []}
              renderItem={(node: Node) => (
                <List.Item>
                  <List.Item.Meta
                    avatar={
                      node.id === selectedCluster.controller?.id ? (
                        <CheckCircleOutlined style={{ fontSize: 20, color: '#52c41a' }} />
                      ) : (
                        <ClusterOutlined style={{ fontSize: 20, color: '#1890ff' }} />
                      )
                    }
                    title={
                      <Space>
                        <Text strong>Node {node.id}</Text>
                        {node.id === selectedCluster.controller?.id && (
                          <Tag color="success">Controller</Tag>
                        )}
                      </Space>
                    }
                    description={`${node.host}:${node.port}${node.rack ? ` (Rack: ${node.rack})` : ''}`}
                  />
                </List.Item>
              )}
            />
          </div>
        )}
      </Drawer>
    </div>
  );
};

export default Clusters;
