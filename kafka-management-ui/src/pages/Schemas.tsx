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
  Select,
  message,
  Tooltip,
  Popconfirm,
  Card,
  Drawer,
  Tabs,
  Empty,
  Alert,
} from 'antd';
import {
  PlusOutlined,
  SearchOutlined,
  ReloadOutlined,
  DeleteOutlined,
  EyeOutlined,
  FileTextOutlined,
  HistoryOutlined,
  DisconnectOutlined,
} from '@ant-design/icons';
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query';
import { useClusterStore } from '../store/clusterStore';
import { schemaApi } from '../api';
import { mockSchemas } from '../api/mockData';
import type { Schema } from '../api/types';

const { Title } = Typography;
const { TextArea } = Input;

const Schemas: React.FC = () => {
  const queryClient = useQueryClient();
  const { selectedClusterId, getSelectedCluster } = useClusterStore();
  const cluster = getSelectedCluster();
  const [searchText, setSearchText] = useState('');
  const [createModalOpen, setCreateModalOpen] = useState(false);
  const [detailDrawerOpen, setDetailDrawerOpen] = useState(false);
  const [selectedSubject, setSelectedSubject] = useState<string | null>(null);
  const [form] = Form.useForm();

  const isConnected = cluster?.status === 'CONNECTED';
  const hasSchemaRegistry = cluster?.hasSchemaRegistry;

  const { data: schemas, isLoading, error, refetch } = useQuery({
    queryKey: ['schemas', selectedClusterId],
    queryFn: () => schemaApi.listSchemas(selectedClusterId!),
    enabled: !!selectedClusterId && isConnected && hasSchemaRegistry,
    retry: false,
    staleTime: 30000,
  });

  // Use mock data when disconnected or error
  const displaySchemas = isConnected && hasSchemaRegistry && !error ? (schemas || []) : mockSchemas;

  const { data: schemaVersions } = useQuery({
    queryKey: ['schemaVersions', selectedClusterId, selectedSubject],
    queryFn: () => schemaApi.getSchemaVersions(selectedClusterId!, selectedSubject!),
    enabled: !!selectedClusterId && !!selectedSubject && isConnected && hasSchemaRegistry,
    retry: false,
  });

  const createMutation = useMutation({
    mutationFn: (data: { subject: string; schema: string; schemaType: string }) =>
      schemaApi.createSchema(selectedClusterId!, data.subject, data.schema, data.schemaType),
    onSuccess: () => {
      message.success('Schema created successfully');
      setCreateModalOpen(false);
      form.resetFields();
      queryClient.invalidateQueries({ queryKey: ['schemas', selectedClusterId] });
    },
    onError: (error: Error) => {
      message.error(`Failed to create schema: ${error.message}`);
    },
  });

  const deleteMutation = useMutation({
    mutationFn: (subject: string) => schemaApi.deleteSchema(selectedClusterId!, subject),
    onSuccess: () => {
      message.success('Schema deleted successfully');
      queryClient.invalidateQueries({ queryKey: ['schemas', selectedClusterId] });
    },
    onError: (error: Error) => {
      message.error(`Failed to delete schema: ${error.message}`);
    },
  });

  const filteredSchemas = displaySchemas.filter((schema: Schema) =>
    schema.subject.toLowerCase().includes(searchText.toLowerCase())
  );

  const getSchemaTypeColor = (type: string) => {
    switch (type?.toUpperCase()) {
      case 'AVRO': return 'blue';
      case 'JSON': return 'green';
      case 'PROTOBUF': return 'purple';
      default: return 'default';
    }
  };

  const columns = [
    {
      title: 'Subject',
      dataIndex: 'subject',
      key: 'subject',
      sorter: (a: Schema, b: Schema) => a.subject.localeCompare(b.subject),
      render: (subject: string) => (
        <Space>
          <FileTextOutlined style={{ color: '#1890ff' }} />
          <a onClick={() => {
            setSelectedSubject(subject);
            setDetailDrawerOpen(true);
          }}>{subject}</a>
        </Space>
      ),
    },
    {
      title: 'Type',
      dataIndex: 'schemaType',
      key: 'schemaType',
      width: 120,
      render: (type: string) => (
        <Tag color={getSchemaTypeColor(type)}>{type || 'AVRO'}</Tag>
      ),
    },
    {
      title: 'Version',
      dataIndex: 'version',
      key: 'version',
      width: 100,
    },
    {
      title: 'Schema ID',
      dataIndex: 'id',
      key: 'id',
      width: 120,
    },
    {
      title: 'Compatibility',
      dataIndex: 'compatibility',
      key: 'compatibility',
      width: 150,
      render: (compat: string) => (
        <Tag>{compat || 'BACKWARD'}</Tag>
      ),
    },
    {
      title: 'Actions',
      key: 'actions',
      width: 120,
      render: (_: unknown, record: Schema) => (
        <Space>
          <Tooltip title="View Details">
            <Button
              type="text"
              icon={<EyeOutlined />}
              onClick={() => {
                setSelectedSubject(record.subject);
                setDetailDrawerOpen(true);
              }}
            />
          </Tooltip>
          <Popconfirm
            title="Delete Schema"
            description={`Are you sure you want to delete "${record.subject}"?`}
            onConfirm={() => deleteMutation.mutate(record.subject)}
            okText="Yes"
            cancelText="No"
            okButtonProps={{ danger: true }}
          >
            <Tooltip title="Delete">
              <Button type="text" danger icon={<DeleteOutlined />} disabled={!isConnected} />
            </Tooltip>
          </Popconfirm>
        </Space>
      ),
    },
  ];

  // Find selected schema from display data for drawer
  const selectedSchema = displaySchemas.find((s: Schema) => s.subject === selectedSubject);

  if (!selectedClusterId) {
    return (
      <Card>
        <div style={{ textAlign: 'center', padding: '60px 0' }}>
          <FileTextOutlined style={{ fontSize: 48, color: '#ccc' }} />
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
        <Title level={4} style={{ margin: 0 }}>Schema Registry</Title>
        <Space>
          <Input
            placeholder="Search schemas..."
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
            Register Schema
          </Button>
        </Space>
      </div>

      {!isConnected && (
        <Alert
          message="Cluster Disconnected - Showing Sample Data"
          description="Connect to Kafka broker and Schema Registry to see real data and perform operations."
          type="warning"
          showIcon
          icon={<DisconnectOutlined />}
          style={{ marginBottom: 16 }}
        />
      )}

      <Table
        columns={columns}
        dataSource={filteredSchemas}
        rowKey="subject"
        loading={isLoading && isConnected}
        pagination={{
          showSizeChanger: true,
          showQuickJumper: true,
          showTotal: (total) => `Total ${total} schemas`,
        }}
      />

      {/* Create Schema Modal */}
      <Modal
        title="Register New Schema"
        open={createModalOpen}
        onCancel={() => {
          setCreateModalOpen(false);
          form.resetFields();
        }}
        footer={null}
        width={700}
      >
        <Form
          form={form}
          layout="vertical"
          onFinish={(values) => createMutation.mutate(values)}
        >
          <Form.Item
            name="subject"
            label="Subject"
            rules={[{ required: true, message: 'Please enter subject name' }]}
          >
            <Input placeholder="my-topic-value" />
          </Form.Item>

          <Form.Item
            name="schemaType"
            label="Schema Type"
            initialValue="AVRO"
            rules={[{ required: true }]}
          >
            <Select
              options={[
                { value: 'AVRO', label: 'Avro' },
                { value: 'JSON', label: 'JSON Schema' },
                { value: 'PROTOBUF', label: 'Protobuf' },
              ]}
            />
          </Form.Item>

          <Form.Item
            name="schema"
            label="Schema Definition"
            rules={[{ required: true, message: 'Please enter schema definition' }]}
          >
            <TextArea
              rows={12}
              placeholder={`{
  "type": "record",
  "name": "MyRecord",
  "fields": [
    {"name": "id", "type": "string"},
    {"name": "timestamp", "type": "long"}
  ]
}`}
              style={{ fontFamily: 'monospace' }}
            />
          </Form.Item>

          <Form.Item style={{ marginBottom: 0, textAlign: 'right' }}>
            <Space>
              <Button onClick={() => setCreateModalOpen(false)}>Cancel</Button>
              <Button type="primary" htmlType="submit" loading={createMutation.isPending}>
                Register
              </Button>
            </Space>
          </Form.Item>
        </Form>
      </Modal>

      {/* Schema Detail Drawer */}
      <Drawer
        title={`Schema: ${selectedSubject}`}
        open={detailDrawerOpen}
        onClose={() => {
          setDetailDrawerOpen(false);
          setSelectedSubject(null);
        }}
        width={700}
      >
        <Tabs
          items={[
            {
              key: 'latest',
              label: 'Latest Version',
              children: (
                <div>
                  {(schemaVersions?.[0] || selectedSchema) ? (
                    <Card size="small">
                      <pre style={{ margin: 0, whiteSpace: 'pre-wrap' }}>
                        {(() => {
                          const schema = schemaVersions?.[0]?.schema || selectedSchema?.schema;
                          try {
                            return JSON.stringify(JSON.parse(schema || ''), null, 2);
                          } catch {
                            return schema;
                          }
                        })()}
                      </pre>
                    </Card>
                  ) : (
                    <Empty description="No schema found" />
                  )}
                </div>
              ),
            },
            {
              key: 'versions',
              label: (
                <span>
                  <HistoryOutlined /> Versions
                </span>
              ),
              children: (
                <Table
                  dataSource={schemaVersions || (selectedSchema ? [selectedSchema] : [])}
                  rowKey="version"
                  columns={[
                    { title: 'Version', dataIndex: 'version', key: 'version' },
                    { title: 'Schema ID', dataIndex: 'id', key: 'id' },
                    { title: 'Type', dataIndex: 'schemaType', key: 'schemaType' },
                  ]}
                  size="small"
                />
              ),
            },
          ]}
        />
      </Drawer>
    </div>
  );
};

export default Schemas;
