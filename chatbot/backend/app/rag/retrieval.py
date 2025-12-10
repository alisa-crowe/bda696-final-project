"""
Query routing and retrieval logic.
"""
import re
from typing import List, Dict, Any, Tuple, Optional
from app.config import settings
from app.rag.chroma_client import get_chroma_client
from app.rag.embeddings import get_embedding_model


# MLB team name mapping: keyword/abbreviation -> full team name
TEAM_NAME_MAP = {
    # AL East
    "yankees": "New York Yankees", "nyy": "New York Yankees", "yankee": "New York Yankees", "bronx": "New York Yankees",
    "red sox": "Boston Red Sox", "bosox": "Boston Red Sox", "bos": "Boston Red Sox", "redsox": "Boston Red Sox",
    "orioles": "Baltimore Orioles", "bal": "Baltimore Orioles", "o's": "Baltimore Orioles", "birdland": "Baltimore Orioles",
    "rays": "Tampa Bay Rays", "tampa bay": "Tampa Bay Rays", "tbr": "Tampa Bay Rays",
    "blue jays": "Toronto Blue Jays", "jays": "Toronto Blue Jays", "tor": "Toronto Blue Jays", "toronto": "Toronto Blue Jays",
    # AL Central
    "white sox": "Chicago White Sox", "chisox": "Chicago White Sox", "chw": "Chicago White Sox", "chicago white sox": "Chicago White Sox",
    "guardians": "Cleveland Guardians", "indians": "Cleveland Guardians", "cle": "Cleveland Guardians", "cleveland": "Cleveland Guardians",
    "tigers": "Detroit Tigers", "det": "Detroit Tigers", "detroit": "Detroit Tigers",
    "royals": "Kansas City Royals", "kcr": "Kansas City Royals", "kansas city": "Kansas City Royals",
    "twins": "Minnesota Twins", "min": "Minnesota Twins", "minnesota": "Minnesota Twins",
    # AL West
    "astros": "Houston Astros", "hou": "Houston Astros", "houston": "Houston Astros",
    "rangers": "Texas Rangers", "tex": "Texas Rangers", "texas": "Texas Rangers",
    "mariners": "Seattle Mariners", "sea": "Seattle Mariners", "seattle": "Seattle Mariners",
    "athletics": "Oakland Athletics", "a's": "Oakland Athletics", "oak": "Oakland Athletics", "oakland": "Oakland Athletics",
    "angels": "Los Angeles Angels", "laa": "Los Angeles Angels", "los angeles angels": "Los Angeles Angels", "halos": "Los Angeles Angels",
    # NL East
    "braves": "Atlanta Braves", "atl": "Atlanta Braves", "atlanta": "Atlanta Braves",
    "mets": "New York Mets", "nym": "New York Mets", "new york mets": "New York Mets",
    "phillies": "Philadelphia Phillies", "phi": "Philadelphia Phillies", "philadelphia": "Philadelphia Phillies",
    "marlins": "Miami Marlins", "mia": "Miami Marlins", "miami": "Miami Marlins",
    "nationals": "Washington Nationals", "nats": "Washington Nationals", "wsn": "Washington Nationals", "washington": "Washington Nationals",
    # NL Central
    "cubs": "Chicago Cubs", "chc": "Chicago Cubs", "chicago cubs": "Chicago Cubs",
    "cardinals": "St. Louis Cardinals", "stl": "St. Louis Cardinals", "cards": "St. Louis Cardinals", "redbirds": "St. Louis Cardinals",
    "brewers": "Milwaukee Brewers", "mil": "Milwaukee Brewers", "milwaukee": "Milwaukee Brewers",
    "pirates": "Pittsburgh Pirates", "pit": "Pittsburgh Pirates", "bucs": "Pittsburgh Pirates", "buccos": "Pittsburgh Pirates",
    "reds": "Cincinnati Reds", "cin": "Cincinnati Reds", "cincinnati": "Cincinnati Reds",
    # NL West
    "dodgers": "Los Angeles Dodgers", "lad": "Los Angeles Dodgers", "la dodgers": "Los Angeles Dodgers",
    "giants": "San Francisco Giants", "sfg": "San Francisco Giants", "sf giants": "San Francisco Giants", "san francisco": "San Francisco Giants",
    "padres": "San Diego Padres", "sdp": "San Diego Padres", "san diego": "San Diego Padres",
    "diamondbacks": "Arizona Diamondbacks", "d-backs": "Arizona Diamondbacks", "ari": "Arizona Diamondbacks", "arizona": "Arizona Diamondbacks",
    "rockies": "Colorado Rockies", "col": "Colorado Rockies", "colorado": "Colorado Rockies",
}

# Set of all team keywords for quick lookup
MLB_TEAMS = set(TEAM_NAME_MAP.keys())

# Common player name patterns (handles "First Last", "First M. Last", "A. J. Cole" style names)
PLAYER_NAME_PATTERN = re.compile(
    r'\b([A-Z][a-z]+(?:\s+[A-Z]\.?\s*)?[A-Z][a-z]+)\b|'  # "Mike Trout" or "A. J. Cole"
    r'\b([A-Z]\.\s*[A-Z]\.\s*[A-Z][a-z]+)\b'  # "A. J. Cole" format
)


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
    normalized_team_name = None
    for team_keyword in MLB_TEAMS:
        if team_keyword in query_lower:
            team_mentioned = team_keyword
            normalized_team_name = TEAM_NAME_MAP[team_keyword]
            break

    if team_mentioned and normalized_team_name:
        # Prioritize teams collection and add metadata filter
        target_collections.insert(0, settings.COLLECTION_TEAMS)  # Put teams first
        target_collections.append(settings.COLLECTION_FAN_POSTS)
        target_collections.append(settings.COLLECTION_GLOBAL_INSIGHTS)
        # Add metadata filter for exact team match
        filters["team_name"] = normalized_team_name

    # Check for player name (simple pattern matching)
    player_match = PLAYER_NAME_PATTERN.search(query)
    if player_match:
        # Extract player name (handle both pattern groups)
        player_name = player_match.group(1) or player_match.group(2)
        if player_name:
            # Prioritize players collection and add metadata filter
            target_collections.insert(0, settings.COLLECTION_PLAYERS)  # Put players first
            target_collections.append(settings.COLLECTION_FAN_POSTS)
            target_collections.append(settings.COLLECTION_GLOBAL_INSIGHTS)
            # Add metadata filter for exact player match
            filters["player_name"] = player_name.strip()

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
            # Only apply filters to collections where they make sense
            where = None
            if filters:
                # For teams collection, filter by team_name if present
                if collection_name == settings.COLLECTION_TEAMS and "team_name" in filters:
                    where = {"team_name": filters["team_name"]}
                # For players collection, filter by player_name if present
                elif collection_name == settings.COLLECTION_PLAYERS and "player_name" in filters:
                    where = {"player_name": filters["player_name"]}
                # For fan_posts, can also filter by team/player
                elif collection_name == settings.COLLECTION_FAN_POSTS:
                    if "team_name" in filters:
                        where = {"team_name": filters["team_name"]}
                    elif "player_name" in filters:
                        where = {"player_name": filters["player_name"]}

            # Increase results for primary collection when filtering
            query_n_results = n_results * 3 if where else n_results

            # Query collection with filter first
            results = collection.query(
                query_embeddings=[query_embedding],
                n_results=query_n_results,
                where=where
            )

            # If filtering returned no results, try without filter (fallback)
            if where and (not results or not results.get("ids") or len(results["ids"][0]) == 0):
                # Fallback: try semantic search without filter
                results = collection.query(
                    query_embeddings=[query_embedding],
                    n_results=n_results,
                    where=None
                )

            # Process results
            if results and results.get("ids") and len(results["ids"][0]) > 0:
                for idx in range(len(results["ids"][0])):
                    doc_id = results["ids"][0][idx]
                    text = results["documents"][0][idx]
                    metadata = results["metadatas"][0][idx] if results.get("metadatas") else {}
                    distance = results["distances"][0][idx] if results.get("distances") else None

                    # Boost exact matches from teams/players collections
                    boost = 0.0
                    if collection_name == settings.COLLECTION_TEAMS and filters.get("team_name"):
                        if metadata.get("team_name") == filters["team_name"]:
                            boost = 0.5  # Boost exact team matches
                    elif collection_name == settings.COLLECTION_PLAYERS and filters.get("player_name"):
                        if metadata.get("player_name") == filters["player_name"]:
                            boost = 0.5  # Boost exact player matches

                    adjusted_distance = (distance if distance is not None else 1.0) - boost

                    all_results.append({
                        "id": doc_id,
                        "text": text,
                        "metadata": metadata,
                        "collection": collection_name,
                        "distance": distance,
                        "adjusted_distance": adjusted_distance,  # For sorting
                        "similarity": 1.0 - distance if distance is not None else None
                    })
        except Exception as e:
            print(f"Warning: Error querying collection {collection_name}: {e}")
            continue

    # Sort by adjusted distance (exact matches first, then by similarity)
    all_results.sort(key=lambda x: x.get("adjusted_distance", x.get("distance", float('inf'))))

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
