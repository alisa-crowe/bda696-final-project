import React from 'react';
import type { MessageWithSources } from '../types';

interface ChatMessageProps {
  message: MessageWithSources;
}

export const ChatMessageComponent: React.FC<ChatMessageProps> = ({ message }) => {
  const isUser = message.role === 'user';

  return (
    <div className={`flex ${isUser ? 'justify-end' : 'justify-start'} mb-4`}>
      <div className={`max-w-3xl ${isUser ? 'order-2' : 'order-1'}`}>
        <div
          className={`rounded-lg px-4 py-3 ${
            isUser
              ? 'bg-primary-600 text-white'
              : 'bg-gray-100 text-gray-900'
          }`}
        >
          <div className="whitespace-pre-wrap">{message.content}</div>
          {message.sources && message.sources.length > 0 && (
            <div className="mt-3 pt-3 border-t border-gray-300 dark:border-gray-600">
              <div className="text-xs font-semibold mb-2 text-gray-600 dark:text-gray-400">
                Sources ({message.sources.length}):
              </div>
              <div className="space-y-1">
                {message.sources.slice(0, 5).map((source, idx) => (
                  <div
                    key={idx}
                    className="text-xs text-gray-500 dark:text-gray-500 flex items-start gap-2"
                  >
                    <span className="font-medium">
                      {source.collection}
                    </span>
                    {source.team_name && (
                      <span className="text-primary-600">• {source.team_name}</span>
                    )}
                    {source.player_name && (
                      <span className="text-primary-600">• {source.player_name}</span>
                    )}
                    {source.doc_type && (
                      <span className="text-gray-400">• {source.doc_type}</span>
                    )}
                  </div>
                ))}
              </div>
            </div>
          )}
        </div>
        <div className={`text-xs text-gray-500 mt-1 ${isUser ? 'text-right' : 'text-left'}`}>
          {message.timestamp.toLocaleTimeString()}
        </div>
      </div>
    </div>
  );
};
