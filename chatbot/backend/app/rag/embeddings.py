"""
Embedding model abstraction.
Supports both Ollama embeddings and sentence-transformers.
"""
from typing import List
import httpx
from app.config import settings


class EmbeddingModel:
    """Unified embedding interface."""

    def __init__(self):
        self.provider = settings.EMBEDDING_PROVIDER
        self.model_name = settings.EMBEDDING_MODEL_NAME
        self._model = None

        if self.provider == "sentence-transformers":
            self._init_sentence_transformers()
        elif self.provider == "ollama":
            # Ollama is called via HTTP, no local model needed
            pass
        else:
            raise ValueError(
                f"Unknown embedding provider: {self.provider}. "
                "Must be 'ollama' or 'sentence-transformers'"
            )

    def _init_sentence_transformers(self):
        """Initialize sentence-transformers model."""
        try:
            from sentence_transformers import SentenceTransformer
            self._model = SentenceTransformer(self.model_name)
        except ImportError:
            raise ImportError(
                "sentence-transformers not installed. "
                "Install with: pip install sentence-transformers"
            )

    def get_embedding(self, text: str) -> List[float]:
        """
        Get embedding for a single text string.

        Args:
            text: Input text to embed

        Returns:
            List of floats representing the embedding vector
        """
        if self.provider == "sentence-transformers":
            return self._model.encode(text, convert_to_numpy=True).tolist()
        elif self.provider == "ollama":
            return self._get_ollama_embedding(text)
        else:
            raise ValueError(f"Unknown provider: {self.provider}")

    def _get_ollama_embedding(self, text: str) -> List[float]:
        """Get embedding from Ollama API."""
        try:
            with httpx.Client(timeout=30.0) as client:
                response = client.post(
                    f"{settings.OLLAMA_BASE_URL}/api/embeddings",
                    json={
                        "model": self.model_name,
                        "prompt": text
                    }
                )
                response.raise_for_status()
                data = response.json()
                return data.get("embedding", [])
        except Exception as e:
            raise RuntimeError(
                f"Failed to get embedding from Ollama: {e}. "
                f"Make sure Ollama is running at {settings.OLLAMA_BASE_URL} "
                f"and model '{self.model_name}' is available."
            )

    def get_embeddings(self, texts: List[str]) -> List[List[float]]:
        """
        Get embeddings for multiple texts.

        Args:
            texts: List of input texts

        Returns:
            List of embedding vectors
        """
        if self.provider == "sentence-transformers":
            return self._model.encode(texts, convert_to_numpy=True).tolist()
        elif self.provider == "ollama":
            # Ollama doesn't batch, so call sequentially
            return [self._get_ollama_embedding(text) for text in texts]
        else:
            raise ValueError(f"Unknown provider: {self.provider}")


# Global embedding model instance
_embedding_model: EmbeddingModel = None


def get_embedding_model() -> EmbeddingModel:
    """Get or create the global embedding model instance."""
    global _embedding_model
    if _embedding_model is None:
        _embedding_model = EmbeddingModel()
    return _embedding_model


def get_embedding(text: str) -> List[float]:
    """Convenience function to get embedding for a single text."""
    return get_embedding_model().get_embedding(text)
