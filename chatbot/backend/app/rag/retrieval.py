"""
Query routing and retrieval logic.
"""
import re
from typing import List, Dict, Any, Tuple, Optional
from app.config import settings
from app.rag.chroma_client import get_chroma_client
from app.rag.embeddings import get_embedding_model


# MLB team names and abbreviations for routing
MLB_TEAMS = {
    "yankees", "nyy", "yankee", "bronx",
    "red sox", "bosox", "bos", "redsox",
    "orioles", "bal", "o's", "birdland",
    "rays", "tampa bay", "tbr",
    "blue jays", "jays", "tor", "toronto",
    "white sox", "chisox", "chw", "chicago white sox",
    "guardians", "indians", "cle", "cleveland",
    "tigers", "det", "detroit",
    "royals", "kcr", "kansas city",
    "twins", "min", "minnesota",
    "astros", "hou", "houston",
    "rangers", "tex", "texas",
    "mariners", "sea", "seattle",
    "athletics", "a's", "oak", "oakland",
    "angels", "laa", "los angeles angels", "halos",
    "braves", "atl", "atlanta",
    "mets", "nym", "new york mets",
    "phillies", "phi", "philadelphia",
    "marlins", "mia", "miami",
    "nationals", "nats", "wsn", "washington",
    "cubs", "chc", "chicago cubs",
    "cardinals", "stl", "cards", "redbirds",
    "brewers", "mil", "milwaukee",
    "pirates", "pit", "bucs", "buccos",
    "reds", "cin", "cincinnati",
    "dodgers", "lad", "la dodgers",
    "giants", "sfg", "sf giants", "san francisco",
    "padres", "sdp", "san diego",
    "diamondbacks", "d-backs", "ari", "arizona",
    "rockies", "col", "colorado",
}

# Common player name patterns
PLAYER_NAME_PATTERN = re.compile(r'\b([A-Z][a-z]+ [A-Z][a-z]+)\b')


def route_query(query: str) -> Tuple[List[str], Dict[str, Any]]:
    """
    Route a query to appropriate collections and determine filters.

    Args:
        query: User query string

    Returns:
        Tuple of (target_collections, filters_dict)
    """
    query_lower = query.lower()
    target_collections = []
    filters = {}

    # Check for FAQ/meta questions
    faq_keywords = [
        "what is", "how do", "how does", "explain", "define",
        "what does", "tell me about", "help", "how to"
    ]
    if any(keyword in query_lower for keyword in faq_keywords):
        target_collections.append(settings.COLLECTION_FAQ)
        target_collections.append(settings.COLLECTION_GLOSSARY)

    # Check for metric/stat definitions
    metric_keywords = ["wrc+", "war", "era", "whip", "ops", "obp", "slg", "fip", "babip"]
    if any(keyword in query_lower for keyword in metric_keywords):
        target_collections.append(settings.COLLECTION_GLOSSARY)
        target_collections.append(settings.COLLECTION_EDGE_CASES)

    # Check for team mentions
    team_mentioned = None
    for team in MLB_TEAMS:
        if team in query_lower:
            team_mentioned = team
            break

    if team_mentioned:
        target_collections.append(settings.COLLECTION_TEAMS)
        target_collections.append(settings.COLLECTION_FAN_POSTS)
        target_collections.append(settings.COLLECTION_GLOBAL_INSIGHTS)
        # Try to normalize team name for filter
        # (This is simplified - you might want a proper team name mapper)

    # Check for player name (simple pattern matching)
    player_match = PLAYER_NAME_PATTERN.search(query)
    if player_match:
        target_collections.append(settings.COLLECTION_PLAYERS)
        target_collections.append(settings.COLLECTION_FAN_POSTS)
        target_collections.append(settings.COLLECTION_GLOBAL_INSIGHTS)

    # Check for sentiment/emotion/theme keywords
    sentiment_keywords = [
        "sentiment", "emotion", "feeling", "mood", "fanbase",
        "volatility", "positive", "negative", "anger", "joy", "fear"
    ]
    if any(keyword in query_lower for keyword in sentiment_keywords):
        target_collections.append(settings.COLLECTION_GLOBAL_INSIGHTS)
        target_collections.append(settings.COLLECTION_TEAMS)
        target_collections.append(settings.COLLECTION_FAN_POSTS)

    # Check for methodology/limitations questions
    meta_keywords = ["methodology", "how is", "how are", "limitation", "cannot", "can't"]
    if any(keyword in query_lower for keyword in meta_keywords):
        target_collections.append(settings.COLLECTION_EDGE_CASES)
        target_collections.append(settings.COLLECTION_FAQ)

    # Default: search all collections if no specific routing
    if not target_collections:
        target_collections = [
            settings.COLLECTION_TEAMS,
            settings.COLLECTION_PLAYERS,
            settings.COLLECTION_FAN_POSTS,
            settings.COLLECTION_GLOBAL_INSIGHTS,
            settings.COLLECTION_GLOSSARY,
            settings.COLLECTION_FAQ,
        ]

    # Remove duplicates while preserving order
    target_collections = list(dict.fromkeys(target_collections))

    return target_collections, filters


def retrieve(
    query: str,
    collections: Optional[List[str]] = None,
    filters: Optional[Dict[str, Any]] = None,
    n_results: int = None
) -> List[Dict[str, Any]]:
    """
    Retrieve relevant documents from Chroma collections.

    Args:
        query: User query string
        collections: List of collection names to search (if None, routes automatically)
        filters: Optional metadata filters
        n_results: Number of results per collection (defaults to config)

    Returns:
        List of retrieved documents with metadata
    """
    if n_results is None:
        n_results = settings.DEFAULT_N_RESULTS

    if collections is None:
        collections, filters = route_query(query)

    chroma = get_chroma_client()
    embedding_model = get_embedding_model()

    # Embed query
    query_embedding = embedding_model.get_embedding(query)

    all_results = []

    # Search each collection
    for collection_name in collections:
        collection = chroma.get_collection(collection_name)
        if collection is None:
            continue

        try:
            # Build where clause from filters
            where = filters or {}

            # Query collection
            results = collection.query(
                query_embeddings=[query_embedding],
                n_results=n_results,
                where=where if where else None
            )

            # Process results
            if results and results.get("ids") and len(results["ids"][0]) > 0:
                for idx in range(len(results["ids"][0])):
                    doc_id = results["ids"][0][idx]
                    text = results["documents"][0][idx]
                    metadata = results["metadatas"][0][idx] if results.get("metadatas") else {}
                    distance = results["distances"][0][idx] if results.get("distances") else None

                    all_results.append({
                        "id": doc_id,
                        "text": text,
                        "metadata": metadata,
                        "collection": collection_name,
                        "distance": distance,
                        "similarity": 1.0 - distance if distance is not None else None
                    })
        except Exception as e:
            print(f"Warning: Error querying collection {collection_name}: {e}")
            continue

    # Sort by similarity (lower distance = higher similarity)
    all_results.sort(key=lambda x: x.get("distance", float('inf')))

    return all_results


def format_context(results: List[Dict[str, Any]], max_chars: int = None) -> str:
    """
    Format retrieved results into a context string for the LLM.

    Args:
        results: List of retrieved documents
        max_chars: Maximum characters in context (defaults to config)

    Returns:
        Formatted context string
    """
    if max_chars is None:
        max_chars = settings.MAX_CONTEXT_CHARS

    context_parts = []
    current_length = 0

    for result in results:
        text = result["text"]
        metadata = result.get("metadata", {})
        collection = result.get("collection", "unknown")

        # Build context entry
        entry = f"[Source: {collection}"
        if metadata.get("team_name"):
            entry += f", Team: {metadata['team_name']}"
        if metadata.get("player_name"):
            entry += f", Player: {metadata['player_name']}"
        if metadata.get("section"):
            entry += f", Section: {metadata['section']}"
        entry += "]\n"
        entry += text
        entry += "\n\n"

        entry_length = len(entry)
        if current_length + entry_length > max_chars:
            break

        context_parts.append(entry)
        current_length += entry_length

    return "".join(context_parts)
