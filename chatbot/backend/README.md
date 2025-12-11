# MLB Analytics Chatbot Backend

RAG-based backend for an MLB baseball analytics chatbot using FastAPI, Chroma, and Ollama.

## Features

- **RAG Pipeline**: Retrieval-Augmented Generation using Chroma vector database
- **Multiple Collections**: Teams, players, fan posts, global insights, glossary, FAQ, edge cases
- **Flexible Embeddings**: Supports Ollama embeddings or sentence-transformers
- **Query Routing**: Intelligent routing to relevant collections based on query content
- **Ollama Integration**: Uses Ollama for LLM inference (e.g., llama3:8b)

## Prerequisites

1. **Python 3.9+**
2. **Ollama** installed and running
   - Install from: https://ollama.ai
   - Pull a model: `ollama pull llama3:8b`
   - For embeddings: `ollama pull nomic-embed-text`

## Installation

1. **Install dependencies:**
   ```bash
   cd chatbot/backend
   pip install -r requirements.txt
   ```

2. **Build the index:**
   ```bash
   python -m scripts.build_index
   ```

   To rebuild from scratch:
   ```bash
   python -m scripts.build_index --rebuild
   ```

## Configuration

Configuration is managed via environment variables (see `app/config.py`):

- `DOCUMENTS_DIR`: Path to documents directory (default: `../documents`)
- `CHROMA_DB_DIR`: Path to Chroma database (default: `./chroma_db`)
- `OLLAMA_BASE_URL`: Ollama API URL (default: `http://localhost:11434`)
- `OLLAMA_MODEL_NAME`: LLM model name (default: `llama3:8b`)
- `EMBEDDING_PROVIDER`: `"ollama"` or `"sentence-transformers"` (default: `"ollama"`)
- `EMBEDDING_MODEL_NAME`: Embedding model name (default: `nomic-embed-text`)
- `DEFAULT_N_RESULTS`: Results per collection (default: `6`)
- `MAX_CONTEXT_CHARS`: Max context length (default: `6000`)

## Running the API

```bash
cd chatbot/backend
uvicorn app.main:app --reload --port 8000
```

The API will be available at `http://localhost:8000`

## API Endpoints

### `GET /health`

Health check endpoint.

**Response:**
```json
{
  "status": "healthy",
  "ollama_connected": true,
  "chroma_connected": true
}
```

### `POST /chat`

Main chat endpoint.

**Request:**
```json
{
  "message": "Which teams have the most positive fanbases?",
  "history": [
    {
      "role": "user",
      "content": "Hello"
    },
    {
      "role": "assistant",
      "content": "Hello! How can I help you with MLB analytics?"
    }
  ]
}
```

**Response:**
```json
{
  "answer": "Based on the sentiment analysis in my knowledge base...",
  "sources": [
    {
      "collection": "teams",
      "doc_type": "team_summary",
      "source_file": "team_insights/team_docs.jsonl",
      "team_name": "Atlanta Braves",
      "player_name": null,
      "section": null
    }
  ]
}
```

## Project Structure

```
backend/
├── app/
│   ├── __init__.py
│   ├── main.py              # FastAPI app
│   ├── config.py            # Configuration
│   ├── models/
│   │   ├── __init__.py
│   │   └── schemas.py       # Pydantic models
│   ├── rag/
│   │   ├── __init__.py
│   │   ├── chroma_client.py # Chroma client
│   │   ├── embeddings.py    # Embedding abstraction
│   │   ├── indexing.py      # Document indexing
│   │   ├── retrieval.py     # Query routing & retrieval
│   │   ├── prompting.py     # Prompt construction
│   │   └── ollama_client.py # Ollama client
│   └── routes/
│       ├── __init__.py
│       ├── health.py        # Health endpoint
│       └── chat.py          # Chat endpoint
├── scripts/
│   ├── __init__.py
│   └── build_index.py       # Indexing script
├── requirements.txt
└── README.md
```

## Chroma Collections

The index uses the following collections:

- `teams`: Team summaries with performance and sentiment metrics
- `players`: Player summaries with statistics
- `fan_posts`: Example fan posts from social media
- `global_insights`: Global analysis documents
- `glossary`: Data dictionary and term definitions
- `faq`: Frequently asked questions
- `edge_cases`: Limitations and methodology documents

## Query Routing

The retrieval system automatically routes queries to relevant collections:

- **Team queries**: Routes to `teams`, `fan_posts`, `global_insights`
- **Player queries**: Routes to `players`, `fan_posts`, `global_insights`
- **Metric definitions**: Routes to `glossary`, `edge_cases`
- **FAQ/meta questions**: Routes to `faq`, `glossary`
- **Sentiment/emotion**: Routes to `global_insights`, `teams`, `fan_posts`

## Troubleshooting

### Ollama Connection Errors

- Ensure Ollama is running: `ollama serve`
- Check that the model is available: `ollama list`
- Verify `OLLAMA_BASE_URL` matches your Ollama instance

### Embedding Errors

- For Ollama embeddings: Ensure `nomic-embed-text` is pulled: `ollama pull nomic-embed-text`
- For sentence-transformers: The model will download automatically on first use

### Index Not Found

- Run `python -m scripts.build_index` to create the index
- Check that `DOCUMENTS_DIR` points to the correct directory

### No Results Retrieved

- Verify documents are indexed: Check `chroma_db/` directory
- Try rebuilding the index: `python -m scripts.build_index --rebuild`
- Check query routing logic in `app/rag/retrieval.py`

## Development

To extend the backend:

1. **Add new collections**: Update `app/config.py` and `app/rag/indexing.py`
2. **Improve routing**: Modify `route_query()` in `app/rag/retrieval.py`
3. **Customize prompts**: Edit `SYSTEM_PROMPT` in `app/rag/prompting.py`
4. **Add endpoints**: Create new routes in `app/routes/`