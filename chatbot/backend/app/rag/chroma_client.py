"""
Chroma vector database client and collection management.
"""
from typing import List, Dict, Any, Optional
import chromadb
from chromadb.config import Settings as ChromaSettings
from chromadb import Collection
from app.config import settings


class ChromaClient:
    """Chroma client wrapper with collection helpers."""

    def __init__(self):
        """Initialize Chroma client."""
        self.client = chromadb.PersistentClient(
            path=str(settings.CHROMA_DB_DIR),
            settings=ChromaSettings(anonymized_telemetry=False)
        )

    def get_or_create_collection(
        self,
        name: str,
        metadata: Optional[Dict[str, Any]] = None
    ) -> Collection:
        """
        Get existing collection or create if it doesn't exist.

        Args:
            name: Collection name
            metadata: Optional metadata for the collection

        Returns:
            Chroma Collection object
        """
        try:
            return self.client.get_collection(name=name)
        except Exception:
            # Chroma doesn't allow empty metadata dict, so only pass if it has content
            if metadata and len(metadata) > 0:
                return self.client.create_collection(
                    name=name,
                    metadata=metadata
                )
            else:
                # Create without metadata if it's empty/None
                return self.client.create_collection(name=name)

    def delete_collection(self, name: str) -> None:
        """Delete a collection by name."""
        try:
            self.client.delete_collection(name=name)
        except Exception:
            pass  # Collection may not exist

    def list_collections(self) -> List[str]:
        """List all collection names."""
        return [c.name for c in self.client.list_collections()]

    def get_collection(self, name: str) -> Optional[Collection]:
        """Get a collection by name, returns None if not found."""
        try:
            return self.client.get_collection(name=name)
        except Exception:
            return None

    def collection_exists(self, name: str) -> bool:
        """Check if a collection exists."""
        return self.get_collection(name) is not None


# Global Chroma client instance
_chroma_client: ChromaClient = None


def get_chroma_client() -> ChromaClient:
    """Get or create the global Chroma client instance."""
    global _chroma_client
    if _chroma_client is None:
        _chroma_client = ChromaClient()
    return _chroma_client
