"""
Ollama LLM client for generating answers.
"""
import httpx
from typing import Optional, List
from app.config import settings
from app.models.schemas import ChatMessage


class OllamaClient:
    """Client for interacting with Ollama API."""

    def __init__(self):
        self.base_url = settings.OLLAMA_BASE_URL
        self.model_name = settings.OLLAMA_MODEL_NAME

    def generate_answer(
        self,
        system_prompt: str,
        context: str,
        user_query: str,
        history: Optional[List[ChatMessage]] = None
    ) -> str:
        """
        Generate an answer using Ollama.

        Args:
            system_prompt: System prompt/instructions
            context: Retrieved context from RAG
            user_query: User's current query
            history: Optional conversation history

        Returns:
            Generated answer string
        """
        # Build full prompt
        from app.rag.prompting import build_prompt
        full_prompt = build_prompt(context, user_query, history)

        # Prepare messages for chat API
        messages = [
            {
                "role": "system",
                "content": system_prompt
            },
            {
                "role": "user",
                "content": full_prompt
            }
        ]

        try:
            with httpx.Client(timeout=120.0) as client:
                response = client.post(
                    f"{self.base_url}/api/chat",
                    json={
                        "model": self.model_name,
                        "messages": messages,
                        "stream": False,
                        "options": {
                            "temperature": 0.7,
                            "top_p": 0.9,
                        }
                    }
                )
                response.raise_for_status()
                data = response.json()

                # Extract message content
                if "message" in data and "content" in data["message"]:
                    return data["message"]["content"]
                else:
                    raise ValueError(f"Unexpected response format: {data}")

        except httpx.HTTPError as e:
            raise RuntimeError(
                f"Failed to connect to Ollama at {self.base_url}: {e}. "
                f"Make sure Ollama is running and the model '{self.model_name}' is available."
            )
        except Exception as e:
            raise RuntimeError(f"Error generating answer: {e}")

    def check_connection(self) -> bool:
        """
        Check if Ollama is reachable.

        Returns:
            True if Ollama is reachable, False otherwise
        """
        try:
            with httpx.Client(timeout=5.0) as client:
                response = client.get(f"{self.base_url}/api/tags")
                response.raise_for_status()
                return True
        except Exception:
            return False


# Global Ollama client instance
_ollama_client: OllamaClient = None


def get_ollama_client() -> OllamaClient:
    """Get or create the global Ollama client instance."""
    global _ollama_client
    if _ollama_client is None:
        _ollama_client = OllamaClient()
    return _ollama_client
