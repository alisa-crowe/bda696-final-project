from pydantic import BaseModel, Field
from typing import Optional, List, Dict, Any


class ChatMessage(BaseModel):
    """Single chat message."""
    role: str = Field(..., description="Message role: 'user' or 'assistant'")
    content: str = Field(..., description="Message content")


class ChatRequest(BaseModel):
    """Request for /chat endpoint."""
    message: str = Field(..., description="User's message/query")
    history: Optional[List[ChatMessage]] = Field(
        default=None,
        description="Optional conversation history (recent turns)"
    )


class Source(BaseModel):
    """Source document metadata."""
    collection: str = Field(..., description="Chroma collection name")
    doc_type: str = Field(..., description="Document type")
    source_file: Optional[str] = Field(None, description="Source file path")
    team_name: Optional[str] = Field(None, description="Team name if applicable")
    player_name: Optional[str] = Field(None, description="Player name if applicable")
    section: Optional[str] = Field(None, description="Section or topic")


class ChatResponse(BaseModel):
    """Response from /chat endpoint."""
    answer: str = Field(..., description="LLM-generated answer")
    sources: Optional[List[Source]] = Field(
        default=None,
        description="Retrieved source documents"
    )


class HealthResponse(BaseModel):
    """Health check response."""
    status: str = Field(..., description="Service status")
    ollama_connected: bool = Field(..., description="Whether Ollama is reachable")
    chroma_connected: bool = Field(..., description="Whether Chroma is accessible")
