"""
Document indexing pipeline.
Loads documents from chatbot/documents/ and indexes them into Chroma collections.
"""
import json
import re
from pathlib import Path
from typing import List, Dict, Any, Optional, Tuple
from app.config import settings
from app.rag.chroma_client import get_chroma_client
from app.rag.embeddings import get_embedding_model


def chunk_markdown(text: str, max_chunk_size: int = 1000) -> List[Tuple[str, Optional[str]]]:
    """
    Split markdown text into chunks by headings.

    Args:
        text: Markdown text
        max_chunk_size: Maximum characters per chunk

    Returns:
        List of (chunk_text, section_name) tuples
    """
    chunks = []
    lines = text.split('\n')
    current_chunk = []
    current_section = None

    for line in lines:
        # Detect markdown headings
        if line.startswith('#'):
            # Save previous chunk if it exists
            if current_chunk:
                chunk_text = '\n'.join(current_chunk)
                if len(chunk_text.strip()) > 0:
                    chunks.append((chunk_text, current_section))
                current_chunk = []

            # Extract section name from heading
            current_section = line.lstrip('#').strip()
        else:
            current_chunk.append(line)

        # If chunk gets too large, split it
        if len('\n'.join(current_chunk)) > max_chunk_size:
            chunk_text = '\n'.join(current_chunk)
            if len(chunk_text.strip()) > 0:
                chunks.append((chunk_text, current_section))
            current_chunk = []

    # Add final chunk
    if current_chunk:
        chunk_text = '\n'.join(current_chunk)
        if len(chunk_text.strip()) > 0:
            chunks.append((chunk_text, current_section))

    return chunks if chunks else [(text, None)]


def determine_collection(file_path: Path) -> Optional[str]:
    """
    Determine which Chroma collection a file should go to based on path.

    Args:
        file_path: Path to the document file

    Returns:
        Collection name or None if file should be skipped
    """
    path_str = str(file_path).lower()

    if 'team_insights' in path_str or 'team_docs' in path_str:
        return settings.COLLECTION_TEAMS
    elif 'player_insights' in path_str or 'player_docs' in path_str:
        return settings.COLLECTION_PLAYERS
    elif 'examples' in path_str or 'example_posts' in path_str:
        return settings.COLLECTION_FAN_POSTS
    elif 'global_insights' in path_str or 'analysis_insights' in path_str:
        return settings.COLLECTION_GLOBAL_INSIGHTS
    elif 'glossary' in path_str:
        return settings.COLLECTION_GLOSSARY
    elif 'faq' in path_str:
        return settings.COLLECTION_FAQ
    elif 'limitations' in path_str or 'methodology' in path_str:
        return settings.COLLECTION_EDGE_CASES
    else:
        # Default: try to infer from filename
        if 'insight' in path_str:
            return settings.COLLECTION_GLOBAL_INSIGHTS
        return None


def index_jsonl_file(
    file_path: Path,
    collection_name: str,
    text_field: str = "text",
    id_field: str = "id"
) -> int:
    """
    Index a JSONL file into a Chroma collection.

    Args:
        file_path: Path to JSONL file
        collection_name: Target Chroma collection name
        text_field: Field name containing the text content
        id_field: Field name containing the unique ID

    Returns:
        Number of documents indexed
    """
    chroma = get_chroma_client()
    collection = chroma.get_or_create_collection(collection_name)
    
    print(f"    Loading embeddings model...")
    try:
        embedding_model = get_embedding_model()
    except Exception as e:
        raise RuntimeError(f"Failed to initialize embedding model: {e}")

    chunks = []
    ids = []
    texts = []
    metadatas = []

    print(f"    Reading {file_path.name}...")
    with open(file_path, 'r', encoding='utf-8') as f:
        for line_num, line in enumerate(f, 1):
            line = line.strip()
            if not line:
                continue

            try:
                doc = json.loads(line)
            except json.JSONDecodeError as e:
                print(f"    Warning: Skipping invalid JSON at line {line_num}: {e}")
                continue

            # Extract text and ID
            # For FAQ files, combine question + rag_guide + answer_pattern
            if collection_name == settings.COLLECTION_FAQ:
                question = doc.get("question", "")
                rag_guide = doc.get("rag_guide", "")
                answer_pattern = doc.get("answer_pattern", "")
                # Combine into a single text field for embedding
                text_parts = []
                if question:
                    text_parts.append(f"Question: {question}")
                if rag_guide:
                    text_parts.append(f"RAG Guide: {rag_guide}")
                if answer_pattern:
                    text_parts.append(f"Answer Pattern: {answer_pattern}")
                text = "\n\n".join(text_parts)
                if not text:
                    continue
                # Use question as ID if no id field
                doc_id = doc.get(id_field, f"faq_{line_num}")
            else:
                text = doc.get(text_field, "")
                if not text:
                    continue
                doc_id = doc.get(id_field, f"{file_path.stem}_{line_num}")

            # Extract metadata
            metadata = {
                "doc_type": doc.get("type", "faq_entry" if collection_name == settings.COLLECTION_FAQ else "unknown"),
                "source_file": str(file_path.relative_to(settings.DOCUMENTS_DIR)),
            }

            # For FAQ files, add question and rag_guide to metadata
            if collection_name == settings.COLLECTION_FAQ:
                if "question" in doc:
                    metadata["question"] = doc["question"]
                if "rag_guide" in doc:
                    metadata["rag_guide"] = doc["rag_guide"]
                if "answer_pattern" in doc:
                    metadata["answer_pattern"] = doc["answer_pattern"]

            # Add nested metadata fields
            if "metadata" in doc and isinstance(doc["metadata"], dict):
                meta = doc["metadata"]
                # Flatten common fields
                if "team_name" in meta:
                    metadata["team_name"] = meta["team_name"]
                if "player_name" in meta:
                    metadata["player_name"] = meta["player_name"]
                if "season" in meta:
                    metadata["season"] = meta["season"]
                if "sentiment_label" in meta:
                    metadata["sentiment_label"] = meta["sentiment_label"]
                if "emotion_label" in meta:
                    metadata["emotion_label"] = meta["emotion_label"]
                if "theme_label" in meta:
                    metadata["theme_label"] = meta["theme_label"]

            # For fan posts, add category
            if "category" in doc:
                metadata["category"] = doc["category"]

            chunks.append({
                "id": doc_id,
                "text": text,
                "metadata": metadata
            })

    # Batch process embeddings
    if chunks:
        print(f"    Generating embeddings for {len(chunks)} chunks...")
        texts = [chunk["text"] for chunk in chunks]
        try:
            embeddings = embedding_model.get_embeddings(texts)
        except Exception as e:
            raise RuntimeError(f"Failed to generate embeddings: {e}")

        # Prepare for Chroma
        ids = [chunk["id"] for chunk in chunks]
        metadatas = [chunk["metadata"] for chunk in chunks]

        print(f"    Upserting into Chroma collection '{collection_name}'...")
        # Upsert into Chroma
        collection.upsert(
            ids=ids,
            embeddings=embeddings,
            documents=texts,
            metadatas=metadatas
        )

    return len(chunks)


def index_markdown_file(file_path: Path, collection_name: str) -> int:
    """
    Index a markdown file into a Chroma collection.

    Args:
        file_path: Path to markdown file
        collection_name: Target Chroma collection name

    Returns:
        Number of chunks indexed
    """
    chroma = get_chroma_client()
    collection = chroma.get_or_create_collection(collection_name)
    
    print(f"    Loading embeddings model...")
    try:
        embedding_model = get_embedding_model()
    except Exception as e:
        raise RuntimeError(f"Failed to initialize embedding model: {e}")

    print(f"    Reading {file_path.name}...")
    with open(file_path, 'r', encoding='utf-8') as f:
        content = f.read()

    chunks = chunk_markdown(content)
    if not chunks:
        return 0

    ids = []
    texts = []
    metadatas = []
    embeddings = []

    print(f"    Generating embeddings for {len(chunks)} chunks...")
    for idx, (chunk_text, section) in enumerate(chunks):
        chunk_id = f"{file_path.stem}_chunk_{idx}"
        ids.append(chunk_id)
        texts.append(chunk_text)

        metadata = {
            "doc_type": "markdown",
            "source_file": str(file_path.relative_to(settings.DOCUMENTS_DIR)),
        }
        if section:
            metadata["section"] = section

        metadatas.append(metadata)
        try:
            embeddings.append(embedding_model.get_embedding(chunk_text))
        except Exception as e:
            raise RuntimeError(f"Failed to generate embedding for chunk {idx}: {e}")

    # Upsert into Chroma
    if ids:
        print(f"    Upserting into Chroma collection '{collection_name}'...")
        collection.upsert(
            ids=ids,
            embeddings=embeddings,
            documents=texts,
            metadatas=metadatas
        )

    return len(chunks)


def index_all_documents(rebuild: bool = False) -> Dict[str, int]:
    """
    Index all documents from chatbot/documents/ into Chroma.

    Args:
        rebuild: If True, delete existing collections before indexing

    Returns:
        Dictionary mapping collection names to number of documents indexed
    """
    chroma = get_chroma_client()
    documents_dir = settings.DOCUMENTS_DIR

    if not documents_dir.exists():
        raise FileNotFoundError(f"Documents directory not found: {documents_dir}")

    # Collections to index
    collections = [
        settings.COLLECTION_TEAMS,
        settings.COLLECTION_PLAYERS,
        settings.COLLECTION_FAN_POSTS,
        settings.COLLECTION_GLOBAL_INSIGHTS,
        settings.COLLECTION_GLOSSARY,
        settings.COLLECTION_FAQ,
        settings.COLLECTION_EDGE_CASES,
    ]

    # Rebuild if requested
    if rebuild:
        print("Rebuilding collections...")
        for coll_name in collections:
            chroma.delete_collection(coll_name)
            print(f"  Deleted collection: {coll_name}")

    stats = {coll: 0 for coll in collections}

    # Walk documents directory
    files_found = 0
    files_skipped = 0
    
    for file_path in documents_dir.rglob("*"):
        if not file_path.is_file():
            continue

        # Skip hidden files and non-document files
        if file_path.name.startswith('.') or file_path.suffix not in ['.jsonl', '.md', '.txt']:
            continue

        files_found += 1
        collection_name = determine_collection(file_path)
        if not collection_name:
            print(f"Skipping {file_path.relative_to(documents_dir)} (no collection mapping)")
            files_skipped += 1
            continue

        print(f"Indexing {file_path.relative_to(documents_dir)} -> {collection_name}")

        try:
            if file_path.suffix == '.jsonl':
                count = index_jsonl_file(file_path, collection_name)
            elif file_path.suffix in ['.md', '.txt']:
                count = index_markdown_file(file_path, collection_name)
            else:
                continue

            stats[collection_name] += count
            print(f"  ✓ Indexed {count} chunks")
        except Exception as e:
            print(f"  ✗ Error indexing {file_path.name}: {e}")
            import traceback
            traceback.print_exc()
            continue

    print(f"\nFiles found: {files_found}, skipped: {files_skipped}")
    return stats
