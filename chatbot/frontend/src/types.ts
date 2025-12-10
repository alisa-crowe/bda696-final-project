export interface ChatMessage {
  role: 'user' | 'assistant';
  content: string;
}

export interface Source {
  collection: string;
  doc_type: string;
  source_file?: string;
  team_name?: string;
  player_name?: string;
  section?: string;
}

export interface ChatRequest {
  message: string;
  history?: ChatMessage[];
}

export interface ChatResponse {
  answer: string;
  sources?: Source[];
}

export interface HealthResponse {
  status: string;
  ollama_connected: boolean;
  chroma_connected: boolean;
}

export interface MessageWithSources extends ChatMessage {
  sources?: Source[];
  timestamp: Date;
}
