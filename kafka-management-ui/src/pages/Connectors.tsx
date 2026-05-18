import React from 'react';
import { Card, Typography, Empty } from 'antd';
import { ApiOutlined } from '@ant-design/icons';
import { useClusterStore } from '../store/clusterStore';

const { Title } = Typography;

const Connectors: React.FC = () => {
  const { selectedClusterId } = useClusterStore();

  if (!selectedClusterId) {
    return (
      <Card>
        <div style={{ textAlign: 'center', padding: '60px 0' }}>
          <ApiOutlined style={{ fontSize: 48, color: '#ccc' }} />
          <Title level={4} style={{ color: '#999', marginTop: 16 }}>
            Select a Cluster
          </Title>
        </div>
      </Card>
    );
  }

  return (
    <div>
      <Title level={4}>Kafka Connect</Title>
      <Card>
        <Empty 
          description="Kafka Connect management coming soon"
          image={Empty.PRESENTED_IMAGE_SIMPLE}
        />
      </Card>
    </div>
  );
};

export default Connectors;
