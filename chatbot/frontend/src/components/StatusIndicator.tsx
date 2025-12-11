import React from 'react';
import type { HealthResponse } from '../types';

interface StatusIndicatorProps {
  health: HealthResponse | null;
  loading?: boolean;
}

export const StatusIndicator: React.FC<StatusIndicatorProps> = ({ health, loading }) => {
  if (loading) {
    return (
      <div className="flex items-center gap-2 text-sm text-gray-500">
        <div className="w-2 h-2 bg-yellow-400 rounded-full animate-pulse"></div>
        <span>Checking connection...</span>
      </div>
    );
  }

  if (!health) {
    return (
      <div className="flex items-center gap-2 text-sm text-red-500">
        <div className="w-2 h-2 bg-red-500 rounded-full"></div>
        <span>Not connected</span>
      </div>
    );
  }

  const isHealthy = health.status === 'healthy' && health.ollama_connected && health.chroma_connected;

  return (
    <div className="flex items-center gap-2 text-sm">
      <div
        className={`w-2 h-2 rounded-full ${
          isHealthy ? 'bg-green-500' : 'bg-yellow-500'
        }`}
      ></div>
      <span className={isHealthy ? 'text-green-600' : 'text-yellow-600'}>
        {isHealthy ? 'Connected' : 'Degraded'}
      </span>
      {!isHealthy && (
        <span className="text-xs text-gray-500">
          ({!health.ollama_connected && 'Ollama '}
          {!health.chroma_connected && 'Chroma '}disconnected)
        </span>
      )}
    </div>
  );
};
