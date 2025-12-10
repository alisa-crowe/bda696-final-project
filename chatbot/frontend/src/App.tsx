import React, { useState, useEffect, useRef } from 'react';
import { ChatMessageComponent } from './components/ChatMessage';
import { ChatInput } from './components/ChatInput';
import { StatusIndicator } from './components/StatusIndicator';
import { apiClient } from './api/client';
import type { MessageWithSources, ChatMessage, HealthResponse } from './types';

function App() {
  const [messages, setMessages] = useState<MessageWithSources[]>([]);
  const [loading, setLoading] = useState(false);
  const [health, setHealth] = useState<HealthResponse | null>(null);
  const [healthLoading, setHealthLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const messagesEndRef = useRef<HTMLDivElement>(null);

  useEffect(() => {
    checkHealth();
    const interval = setInterval(checkHealth, 30000); // Check every 30 seconds
    return () => clearInterval(interval);
  }, []);

  useEffect(() => {
    scrollToBottom();
  }, [messages]);

  const scrollToBottom = () => {
    messagesEndRef.current?.scrollIntoView({ behavior: 'smooth' });
  };

  const checkHealth = async () => {
    try {
      setHealthLoading(true);
      const healthData = await apiClient.healthCheck();
      setHealth(healthData);
    } catch (err) {
      setHealth(null);
    } finally {
      setHealthLoading(false);
    }
  };

  const handleSendMessage = async (content: string) => {
    if (loading) return;

    const userMessage: MessageWithSources = {
      role: 'user',
      content,
      timestamp: new Date(),
    };

    setMessages((prev) => [...prev, userMessage]);
    setLoading(true);
    setError(null);

    try {
      // Build history from previous messages
      const history: ChatMessage[] = messages.map((msg) => ({
        role: msg.role,
        content: msg.content,
      }));

      const response = await apiClient.sendMessage({
        message: content,
        history,
      });

      const assistantMessage: MessageWithSources = {
        role: 'assistant',
        content: response.answer,
        sources: response.sources,
        timestamp: new Date(),
      };

      setMessages((prev) => [...prev, assistantMessage]);
    } catch (err) {
      const errorMessage = err instanceof Error ? err.message : 'Failed to send message';
      setError(errorMessage);
      
      const errorMsg: MessageWithSources = {
        role: 'assistant',
        content: `Sorry, I encountered an error: ${errorMessage}. Please try again.`,
        timestamp: new Date(),
      };
      setMessages((prev) => [...prev, errorMsg]);
    } finally {
      setLoading(false);
    }
  };

  const handleClearChat = () => {
    setMessages([]);
    setError(null);
  };

  return (
    <div className="flex flex-col h-screen bg-gray-50">
      {/* Header */}
      <header className="bg-white border-b border-gray-200 shadow-sm">
        <div className="max-w-6xl mx-auto px-4 py-4 flex justify-between items-center">
          <div>
            <h1 className="text-2xl font-bold text-gray-900">MLB Analytics Chatbot</h1>
            <p className="text-sm text-gray-600 mt-1">
              Ask about teams, players, stats, and fan sentiment
            </p>
          </div>
          <div className="flex items-center gap-4">
            <StatusIndicator health={health} loading={healthLoading} />
            {messages.length > 0 && (
              <button
                onClick={handleClearChat}
                className="px-4 py-2 text-sm text-gray-600 hover:text-gray-900 hover:bg-gray-100 rounded-lg transition-colors"
              >
                Clear Chat
              </button>
            )}
          </div>
        </div>
      </header>

      {/* Messages Area */}
      <div className="flex-1 overflow-y-auto">
        <div className="max-w-4xl mx-auto px-4 py-6">
          {messages.length === 0 && (
            <div className="text-center mt-12">
              <div className="text-6xl mb-4">⚾</div>
              <h2 className="text-2xl font-semibold text-gray-900 mb-2">
                Welcome to MLB Analytics Chatbot
              </h2>
              <p className="text-gray-600 mb-6">
                Ask me anything about MLB teams, players, statistics, or fan sentiment analysis.
              </p>
              <div className="flex flex-wrap gap-2 justify-center">
                {[
                  'Which teams have the most positive fanbases?',
                  'What is WAR?',
                  'Tell me about the Atlanta Braves',
                  'How are sentiment scores calculated?',
                ].map((example, idx) => (
                  <button
                    key={idx}
                    onClick={() => handleSendMessage(example)}
                    className="px-4 py-2 text-sm bg-white border border-gray-300 rounded-lg hover:bg-gray-50 hover:border-primary-300 transition-colors"
                  >
                    {example}
                  </button>
                ))}
              </div>
            </div>
          )}

          {messages.map((message, idx) => (
            <ChatMessageComponent key={idx} message={message} />
          ))}

          {loading && (
            <div className="flex justify-start mb-4">
              <div className="bg-gray-100 rounded-lg px-4 py-3">
                <div className="flex items-center gap-2">
                  <div className="w-2 h-2 bg-gray-400 rounded-full animate-bounce"></div>
                  <div className="w-2 h-2 bg-gray-400 rounded-full animate-bounce" style={{ animationDelay: '0.2s' }}></div>
                  <div className="w-2 h-2 bg-gray-400 rounded-full animate-bounce" style={{ animationDelay: '0.4s' }}></div>
                  <span className="text-gray-600 ml-2">Thinking...</span>
                </div>
              </div>
            </div>
          )}

          {error && (
            <div className="bg-red-50 border border-red-200 rounded-lg px-4 py-3 text-red-800 text-sm">
              {error}
            </div>
          )}

          <div ref={messagesEndRef} />
        </div>
      </div>

      {/* Input Area */}
      <ChatInput onSendMessage={handleSendMessage} disabled={loading} />
    </div>
  );
}

export default App;
