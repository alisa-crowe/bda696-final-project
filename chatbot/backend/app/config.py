from pathlib import Path
import os
from typing import Literal


class Settings:
    """
    Central configuration for the backend.
    Environment variables override defaults where applicable.
    """

    BASE_DIR: Path = Path(__file__).resolve().parents[1]  # backend/
    CHATBOT_DIR: Path = BASE_DIR.parent  # chatbot/
    DOCUMENTS_DIR: Path = Path(
        os.getenv("DOCUMENTS_DIR", CHATBOT_DIR / "documents")
    )
    CHROMA_DB_DIR: Path = Path(
        os.getenv("CHROMA_DB_DIR", BASE_DIR / "chroma_db")
    )

    OLLAMA_BASE_URL: str = os.getenv("OLLAMA_BASE_URL", "http://localhost:11434")
    OLLAMA_MODEL_NAME: str = os.getenv("OLLAMA_MODEL_NAME", "llama3:8b")

    # Embedding provider: "ollama" (e.g., nomic-embed-text) or "sentence-transformers"
    EMBEDDING_PROVIDER: Literal["ollama", "sentence-transformers"] = os.getenv(
        "EMBEDDING_PROVIDER", "ollama"
    )
    EMBEDDING_MODEL_NAME: str = os.getenv(
        "EMBEDDING_MODEL_NAME", "nomic-embed-text"
    )

    # Retrieval defaults
    DEFAULT_N_RESULTS: int = int(os.getenv("DEFAULT_N_RESULTS", 6))
    MAX_CONTEXT_CHARS: int = int(os.getenv("MAX_CONTEXT_CHARS", 6000))

    # Chroma collection names
    COLLECTION_TEAMS: str = "teams"
    COLLECTION_PLAYERS: str = "players"
    COLLECTION_FAN_POSTS: str = "fan_posts"
    COLLECTION_GLOBAL_INSIGHTS: str = "global_insights"
    COLLECTION_GLOSSARY: str = "glossary"
    COLLECTION_FAQ: str = "faq"
    COLLECTION_EDGE_CASES: str = "edge_cases"


settings = Settings()
