import { create } from 'zustand';
import { persist } from 'zustand/middleware';
import type { Cluster } from '../api/types';
import { clusterApi } from '../api';

interface ClusterState {
  clusters: Cluster[];
  selectedClusterId: string | null;
  loading: boolean;
  error: string | null;
  setClusters: (clusters: Cluster[]) => void;
  setSelectedCluster: (clusterId: string | null) => void;
  getSelectedCluster: () => Cluster | undefined;
  fetchClusters: () => Promise<void>;
}

export const useClusterStore = create<ClusterState>()(
  persist(
    (set, get) => ({
      clusters: [],
      selectedClusterId: null,
      loading: false,
      error: null,
      setClusters: (clusters) => set({ clusters }),
      setSelectedCluster: (clusterId) => set({ selectedClusterId: clusterId }),
      getSelectedCluster: () => {
        const { clusters, selectedClusterId } = get();
        return clusters.find((c) => c.id === selectedClusterId);
      },
      fetchClusters: async () => {
        set({ loading: true, error: null });
        try {
          const clusters = await clusterApi.listClusters();
          set({ clusters, loading: false });
          // Auto-select first cluster if none selected
          const { selectedClusterId } = get();
          if (!selectedClusterId && clusters.length > 0) {
            set({ selectedClusterId: clusters[0].id });
          }
        } catch (error) {
          set({ 
            loading: false, 
            error: error instanceof Error ? error.message : 'Failed to fetch clusters' 
          });
        }
      },
    }),
    {
      name: 'cluster-storage',
      partialize: (state) => ({ selectedClusterId: state.selectedClusterId }),
    }
  )
);
