import { useEffect } from 'react';
import { Routes, Route, Navigate } from 'react-router-dom';
import MainLayout from './layouts/MainLayout';
import Dashboard from './pages/Dashboard';
import Topics from './pages/Topics';
import TopicDetail from './pages/TopicDetail';
import ConsumerGroups from './pages/ConsumerGroups';
import ConsumerGroupDetail from './pages/ConsumerGroupDetail';
import Schemas from './pages/Schemas';
import Connectors from './pages/Connectors';
import ACLs from './pages/ACLs';
import Users from './pages/Users';
import Roles from './pages/Roles';
import Clusters from './pages/Clusters';
import { useClusterStore } from './store/clusterStore';

function App() {
  const { fetchClusters } = useClusterStore();

  useEffect(() => {
    fetchClusters();
  }, [fetchClusters]);

  return (
    <Routes>
      <Route path="/" element={<MainLayout />}>
        <Route index element={<Navigate to="/dashboard" replace />} />
        <Route path="dashboard" element={<Dashboard />} />
        <Route path="topics" element={<Topics />} />
        <Route path="topics/:topicName" element={<TopicDetail />} />
        <Route path="consumer-groups" element={<ConsumerGroups />} />
        <Route path="consumer-groups/:groupId" element={<ConsumerGroupDetail />} />
        <Route path="schemas" element={<Schemas />} />
        <Route path="connectors" element={<Connectors />} />
        <Route path="acls" element={<ACLs />} />
        <Route path="users" element={<Users />} />
        <Route path="roles" element={<Roles />} />
        <Route path="clusters" element={<Clusters />} />
      </Route>
    </Routes>
  );
}

export default App;
