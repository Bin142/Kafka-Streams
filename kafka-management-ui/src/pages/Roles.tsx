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
  Drawer,
  Descriptions,
  List,
  Alert,
} from 'antd';
import {
  PlusOutlined,
  SearchOutlined,
  ReloadOutlined,
  DeleteOutlined,
  EditOutlined,
  EyeOutlined,
  SafetyOutlined,
} from '@ant-design/icons';
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query';
import { roleApi, permissionApi } from '../api';
import { mockRoles, mockPermissions } from '../api/mockData';
import type { Role, Permission } from '../api/types';
import dayjs from 'dayjs';

const { Title } = Typography;
const { TextArea } = Input;

const Roles: React.FC = () => {
  const queryClient = useQueryClient();
  const [searchText, setSearchText] = useState('');
  const [modalOpen, setModalOpen] = useState(false);
  const [detailDrawerOpen, setDetailDrawerOpen] = useState(false);
  const [editingRole, setEditingRole] = useState<Role | null>(null);
  const [selectedRole, setSelectedRole] = useState<Role | null>(null);
  const [form] = Form.useForm();

  const { data: roles, isLoading, error, refetch } = useQuery({
    queryKey: ['roles'],
    queryFn: () => roleApi.listRoles(),
    retry: false,
    staleTime: 30000,
  });

  const { data: permissions } = useQuery({
    queryKey: ['permissions'],
    queryFn: () => permissionApi.listPermissions(),
    retry: false,
    staleTime: 30000,
  });

  // Use mock data when error
  const displayRoles = !error ? (roles || mockRoles) : mockRoles;
  const displayPermissions = permissions || mockPermissions;
  const hasError = !!error;

  const createMutation = useMutation({
    mutationFn: (data: Partial<Role>) => roleApi.createRole(data),
    onSuccess: () => {
      message.success('Role created successfully');
      closeModal();
      queryClient.invalidateQueries({ queryKey: ['roles'] });
    },
    onError: (error: Error) => {
      message.error(`Failed to create role: ${error.message}`);
    },
  });

  const updateMutation = useMutation({
    mutationFn: ({ id, data }: { id: number; data: Partial<Role> }) => roleApi.updateRole(id, data),
    onSuccess: () => {
      message.success('Role updated successfully');
      closeModal();
      queryClient.invalidateQueries({ queryKey: ['roles'] });
    },
    onError: (error: Error) => {
      message.error(`Failed to update role: ${error.message}`);
    },
  });

  const deleteMutation = useMutation({
    mutationFn: (id: number) => roleApi.deleteRole(id),
    onSuccess: () => {
      message.success('Role deleted successfully');
      queryClient.invalidateQueries({ queryKey: ['roles'] });
    },
    onError: (error: Error) => {
      message.error(`Failed to delete role: ${error.message}`);
    },
  });

  const closeModal = () => {
    setModalOpen(false);
    setEditingRole(null);
    form.resetFields();
  };

  const openEditModal = (role: Role) => {
    setEditingRole(role);
    form.setFieldsValue({
      name: role.name,
      description: role.description,
      permissionIds: role.permissions?.map((p) => p.id),
    });
    setModalOpen(true);
  };

  const handleSubmit = (values: Record<string, unknown>) => {
    if (editingRole) {
      updateMutation.mutate({ id: editingRole.id, data: values });
    } else {
      createMutation.mutate(values as Partial<Role>);
    }
  };

  const filteredRoles = displayRoles.filter((role: Role) =>
    role.name.toLowerCase().includes(searchText.toLowerCase()) ||
    role.description?.toLowerCase().includes(searchText.toLowerCase())
  );

  const columns = [
    {
      title: 'Role Name',
      dataIndex: 'name',
      key: 'name',
      sorter: (a: Role, b: Role) => a.name.localeCompare(b.name),
      render: (name: string, record: Role) => (
        <Space>
          <SafetyOutlined style={{ color: record.isSystem ? '#faad14' : '#1890ff' }} />
          <a onClick={() => {
            setSelectedRole(record);
            setDetailDrawerOpen(true);
          }}>{name}</a>
          {record.isSystem && <Tag color="warning">System</Tag>}
        </Space>
      ),
    },
    {
      title: 'Description',
      dataIndex: 'description',
      key: 'description',
      ellipsis: true,
    },
    {
      title: 'Permissions',
      dataIndex: 'permissions',
      key: 'permissions',
      width: 150,
      render: (perms: Permission[]) => (
        <Tag color="blue">{perms?.length || 0} permissions</Tag>
      ),
    },
    {
      title: 'Created',
      dataIndex: 'createdAt',
      key: 'createdAt',
      width: 150,
      render: (date: string) => dayjs(date).format('YYYY-MM-DD'),
    },
    {
      title: 'Actions',
      key: 'actions',
      width: 150,
      render: (_: unknown, record: Role) => (
        <Space>
          <Tooltip title="View Details">
            <Button
              type="text"
              icon={<EyeOutlined />}
              onClick={() => {
                setSelectedRole(record);
                setDetailDrawerOpen(true);
              }}
            />
          </Tooltip>
          <Tooltip title="Edit">
            <Button
              type="text"
              icon={<EditOutlined />}
              onClick={() => openEditModal(record)}
              disabled={record.isSystem || hasError}
            />
          </Tooltip>
          <Popconfirm
            title="Delete Role"
            description={`Are you sure you want to delete "${record.name}"?`}
            onConfirm={() => deleteMutation.mutate(record.id)}
            okText="Yes"
            cancelText="No"
            okButtonProps={{ danger: true }}
          >
            <Tooltip title="Delete">
              <Button
                type="text"
                danger
                icon={<DeleteOutlined />}
                disabled={record.isSystem || hasError}
              />
            </Tooltip>
          </Popconfirm>
        </Space>
      ),
    },
  ];

  return (
    <div>
      <div style={{ display: 'flex', justifyContent: 'space-between', marginBottom: 16 }}>
        <Title level={4} style={{ margin: 0 }}>Roles</Title>
        <Space>
          <Input
            placeholder="Search roles..."
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
            disabled={hasError}
          >
            Create Role
          </Button>
        </Space>
      </div>

      {hasError && (
        <Alert
          message="API Unavailable - Showing Sample Data"
          description="Role management API is not responding. Showing sample data for preview."
          type="warning"
          showIcon
          style={{ marginBottom: 16 }}
        />
      )}

      <Table
        columns={columns}
        dataSource={filteredRoles}
        rowKey="id"
        loading={isLoading}
        pagination={{
          showSizeChanger: true,
          showQuickJumper: true,
          showTotal: (total) => `Total ${total} roles`,
        }}
      />

      {/* Create/Edit Modal */}
      <Modal
        title={editingRole ? 'Edit Role' : 'Create New Role'}
        open={modalOpen}
        onCancel={closeModal}
        footer={null}
        width={600}
      >
        <Form
          form={form}
          layout="vertical"
          onFinish={handleSubmit}
        >
          <Form.Item
            name="name"
            label="Role Name"
            rules={[{ required: true, message: 'Please enter role name' }]}
          >
            <Input disabled={!!editingRole} />
          </Form.Item>

          <Form.Item name="description" label="Description">
            <TextArea rows={3} />
          </Form.Item>

          <Form.Item name="permissionIds" label="Permissions">
            <Select
              mode="multiple"
              placeholder="Select permissions"
              optionFilterProp="label"
              options={displayPermissions.map((p: Permission) => ({
                value: p.id,
                label: `${p.resource}:${p.action}`,
              }))}
            />
          </Form.Item>

          <Form.Item style={{ marginBottom: 0, textAlign: 'right' }}>
            <Space>
              <Button onClick={closeModal}>Cancel</Button>
              <Button
                type="primary"
                htmlType="submit"
                loading={createMutation.isPending || updateMutation.isPending}
              >
                {editingRole ? 'Update' : 'Create'}
              </Button>
            </Space>
          </Form.Item>
        </Form>
      </Modal>

      {/* Role Detail Drawer */}
      <Drawer
        title={`Role: ${selectedRole?.name}`}
        open={detailDrawerOpen}
        onClose={() => {
          setDetailDrawerOpen(false);
          setSelectedRole(null);
        }}
        width={500}
      >
        {selectedRole && (
          <div>
            <Descriptions bordered column={1} style={{ marginBottom: 24 }}>
              <Descriptions.Item label="Name">{selectedRole.name}</Descriptions.Item>
              <Descriptions.Item label="Description">{selectedRole.description || '-'}</Descriptions.Item>
              <Descriptions.Item label="System Role">
                {selectedRole.isSystem ? <Tag color="warning">Yes</Tag> : <Tag>No</Tag>}
              </Descriptions.Item>
              <Descriptions.Item label="Created">
                {dayjs(selectedRole.createdAt).format('YYYY-MM-DD HH:mm:ss')}
              </Descriptions.Item>
            </Descriptions>

            <Title level={5}>Permissions ({selectedRole.permissions?.length || 0})</Title>
            <List
              dataSource={selectedRole.permissions || []}
              renderItem={(perm: Permission) => (
                <List.Item>
                  <List.Item.Meta
                    avatar={<SafetyOutlined style={{ color: '#1890ff' }} />}
                    title={`${perm.resource}:${perm.action}`}
                    description={perm.description}
                  />
                </List.Item>
              )}
              locale={{ emptyText: 'No permissions assigned' }}
            />
          </div>
        )}
      </Drawer>
    </div>
  );
};

export default Roles;
