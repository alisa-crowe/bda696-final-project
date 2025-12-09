"""
Health check endpoint.
"""
from fastapi import APIRouter
from app.models.schemas import HealthResponse
from app.rag.chroma_client import get_chroma_client
from app.rag.ollama_client import get_ollama_client

router = APIRouter()


@router.get("/health", response_model=HealthResponse)
async def health_check():
    """
    Health check endpoint.
    Verifies that Ollama and Chroma are accessible.
    """
    ollama_client = get_ollama_client()
    chroma_client = get_chroma_client()

    ollama_connected = ollama_client.check_connection()
    chroma_connected = chroma_client.client is not None

    status = "healthy" if (ollama_connected and chroma_connected) else "degraded"

    return HealthResponse(
        status=status,
        ollama_connected=ollama_connected,
        chroma_connected=chroma_connected
    )
