# Quick Start Guide - MLB Analytics Chatbot

Get the full chatbot (backend + frontend) running in minutes!

## Prerequisites

- Python 3.8+ with pip
- Node.js 18+ with npm
- Ollama installed and running (`ollama serve`)
- `llama3:8b` model downloaded (`ollama pull llama3:8b`)

## Step 1: Backend Setup

```bash
# Navigate to backend directory
cd chatbot/backend

# Install Python dependencies
pip install -r requirements.txt

# Build the knowledge base index
python -m scripts.build_index --rebuild

# Start the API server
uvicorn app.main:app --reload --port 8000
```

The backend will be running at `http://localhost:8000`

**Keep this terminal open!**

## Step 2: Frontend Setup

Open a **new terminal**:

```bash
# Navigate to frontend directory
cd chatbot/frontend

# Install Node.js dependencies
npm install

# Start the development server
npm run dev
```

The frontend will be running at `http://localhost:3000`

## Step 3: Test It!

1. Open your browser to `http://localhost:3000`
2. You should see the chat interface
3. Try asking:
   - "Which teams have the most positive fanbases?"
   - "What is WAR?"
   - "Tell me about the Atlanta Braves"

## Troubleshooting

### Backend Issues

**"Ollama not connected"**
- Make sure Ollama is running: `ollama serve`
- Check if the model is available: `ollama list`

**"Chroma not connected"**
- Rebuild the index: `python -m scripts.build_index --rebuild`

**Port 8000 already in use**
- Change the port: `uvicorn app.main:app --reload --port 8001`
- Update frontend `.env` file to match

### Frontend Issues

**"Cannot connect to API"**
- Make sure the backend is running on port 8000
- Check browser console for CORS errors
- Verify `VITE_API_URL` in `.env` (or create one)

**"npm install fails"**
- Make sure Node.js 18+ is installed: `node --version`
- Try clearing cache: `rm -rf node_modules package-lock.json && npm install`

**Port 3000 already in use**
- Vite will automatically use the next available port
- Or specify: `npm run dev -- --port 3001`

## Architecture

```
┌─────────────┐         HTTP/REST          ┌─────────────┐
│  Frontend   │ ──────────────────────────> │   Backend   │
│  (React)    │ <────────────────────────── │  (FastAPI)  │
│  Port 3000  │         JSON Responses      │  Port 8000  │
└─────────────┘                              └─────────────┘
                                                      │
                                                      │
                                              ┌───────┴────────┐
                                              │                │
                                         ┌────▼────┐    ┌─────▼─────┐
                                         │ Ollama  │    │  ChromaDB │
                                         │  LLM    │    │  Vector   │
                                         │         │    │  Database │
                                         └─────────┘    └───────────┘
```

## Next Steps

- Customize the UI in `frontend/src/App.tsx`
- Add more documents to `chatbot/documents/`
- Adjust RAG parameters in `backend/app/config.py`
- Deploy to production (see deployment guides in README files)

## Development Workflow

1. **Backend changes**: The server auto-reloads with `--reload` flag
2. **Frontend changes**: Vite hot-reloads automatically
3. **Document updates**: Rebuild index: `python -m scripts.build_index --rebuild`

Enjoy your MLB Analytics Chatbot! ⚾
