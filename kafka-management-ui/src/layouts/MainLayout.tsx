import React, { useState } from 'react';
import { Outlet, useNavigate, useLocation } from 'react-router-dom';
import {
  Layout,
  Menu,
  Select,
  theme,
  Typography,
  Space,
  Badge,
  Avatar,
  Dropdown,
} from 'antd';
import {
  DashboardOutlined,
  DatabaseOutlined,
  TeamOutlined,
  FileTextOutlined,
  ApiOutlined,
  SafetyOutlined,
  UserOutlined,
  SettingOutlined,
  ClusterOutlined,
  MenuFoldOutlined,
  MenuUnfoldOutlined,
} from '@ant-design/icons';
import { useClusterStore } from '../store/clusterStore';

const { Header, Sider, Content } = Layout;
const { Title } = Typography;

const MainLayout: React.FC = () => {
  const [collapsed, setCollapsed] = useState(false);
  const navigate = useNavigate();
  const location = useLocation();
  const { token } = theme.useToken();
  
  const { clusters, selectedClusterId, setSelectedCluster } = useClusterStore();

  const menuItems = [
    {
      key: '/dashboard',
      icon: <DashboardOutlined />,
      label: 'Dashboard',
    },
    {
      key: '/topics',
      icon: <DatabaseOutlined />,
      label: 'Topics',
    },
    {
      key: '/consumer-groups',
      icon: <TeamOutlined />,
      label: 'Consumer Groups',
    },
    {
      key: '/schemas',
      icon: <FileTextOutlined />,
      label: 'Schema Registry',
    },
    {
      key: '/connectors',
      icon: <ApiOutlined />,
      label: 'Kafka Connect',
    },
    {
      key: '/acls',
      icon: <SafetyOutlined />,
      label: 'ACLs',
    },
    {
      type: 'divider' as const,
    },
    {
      key: 'admin',
      icon: <SettingOutlined />,
      label: 'Administration',
      children: [
        {
          key: '/users',
          icon: <UserOutlined />,
          label: 'Users',
        },
        {
          key: '/roles',
          icon: <TeamOutlined />,
          label: 'Roles',
        },
        {
          key: '/clusters',
          icon: <ClusterOutlined />,
          label: 'Clusters',
        },
      ],
    },
  ];

  const userMenuItems = [
    { key: 'profile', label: 'Profile' },
    { key: 'settings', label: 'Settings' },
    { type: 'divider' as const },
    { key: 'logout', label: 'Logout' },
  ];

  const getSelectedKey = () => {
    const path = location.pathname;
    if (path.startsWith('/topics')) return '/topics';
    if (path.startsWith('/consumer-groups')) return '/consumer-groups';
    if (path.startsWith('/schemas')) return '/schemas';
    if (path.startsWith('/connectors')) return '/connectors';
    if (path.startsWith('/acls')) return '/acls';
    if (path.startsWith('/users')) return '/users';
    if (path.startsWith('/roles')) return '/roles';
    if (path.startsWith('/clusters')) return '/clusters';
    return '/dashboard';
  };

  return (
    <Layout style={{ minHeight: '100vh' }}>
      <Sider
        trigger={null}
        collapsible
        collapsed={collapsed}
        width={240}
        style={{
          background: token.colorBgContainer,
          borderRight: `1px solid ${token.colorBorderSecondary}`,
        }}
      >
        <div
          style={{
            height: 64,
            display: 'flex',
            alignItems: 'center',
            justifyContent: collapsed ? 'center' : 'flex-start',
            padding: collapsed ? 0 : '0 16px',
            borderBottom: `1px solid ${token.colorBorderSecondary}`,
          }}
        >
          <ClusterOutlined style={{ fontSize: 24, color: token.colorPrimary }} />
          {!collapsed && (
            <Title level={4} style={{ margin: '0 0 0 12px', color: token.colorPrimary }}>
              Kafka Manager
            </Title>
          )}
        </div>

        <Menu
          mode="inline"
          selectedKeys={[getSelectedKey()]}
          defaultOpenKeys={['admin']}
          items={menuItems}
          onClick={({ key }) => navigate(key)}
          style={{ border: 'none', marginTop: 8 }}
        />
      </Sider>

      <Layout>
        <Header
          style={{
            padding: '0 24px',
            background: token.colorBgContainer,
            borderBottom: `1px solid ${token.colorBorderSecondary}`,
            display: 'flex',
            alignItems: 'center',
            justifyContent: 'space-between',
          }}
        >
          <Space>
            {React.createElement(collapsed ? MenuUnfoldOutlined : MenuFoldOutlined, {
              onClick: () => setCollapsed(!collapsed),
              style: { fontSize: 18, cursor: 'pointer' },
            })}

            <Select
              placeholder="Select Cluster"
              value={selectedClusterId}
              onChange={setSelectedCluster}
              style={{ width: 250 }}
              options={clusters.map((c) => ({
                value: c.id,
                label: (
                  <Space>
                    <Badge
                      status={c.status === 'CONNECTED' ? 'success' : 'error'}
                    />
                    {c.name}
                  </Space>
                ),
              }))}
            />
          </Space>

          <Space size="middle">
            <Dropdown menu={{ items: userMenuItems }} placement="bottomRight">
              <Space style={{ cursor: 'pointer' }}>
                <Avatar icon={<UserOutlined />} />
                <span>Admin</span>
              </Space>
            </Dropdown>
          </Space>
        </Header>

        <Content
          style={{
            margin: 24,
            padding: 24,
            background: token.colorBgContainer,
            borderRadius: token.borderRadiusLG,
            minHeight: 280,
            overflow: 'auto',
          }}
        >
          <Outlet />
        </Content>
      </Layout>
    </Layout>
  );
};

export default MainLayout;
