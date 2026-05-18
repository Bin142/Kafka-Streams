import React from 'react';
import { Card, Typography, Empty } from 'antd';
import { SafetyOutlined } from '@ant-design/icons';
import { useClusterStore } from '../store/clusterStore';

const { Title } = Typography;

const ACLs: React.FC = () => {
  const { selectedClusterId } = useClusterStore();

  if (!selectedClusterId) {
    return (
      <Card>
        <div style={{ textAlign: 'center', padding: '60px 0' }}>
          <SafetyOutlined style={{ fontSize: 48, color: '#ccc' }} />
          <Title level={4} style={{ color: '#999', marginTop: 16 }}>
            Select a Cluster
          </Title>
        </div>
      </Card>
    );
  }

  return (
    <div>
      <Title level={4}>Access Control Lists (ACLs)</Title>
      <Card>
        <Empty 
          description="ACL management coming soon"
          image={Empty.PRESENTED_IMAGE_SIMPLE}
        />
      </Card>
    </div>
  );
};

export default ACLs;
