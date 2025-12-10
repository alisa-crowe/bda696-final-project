"""
FastAPI application entrypoint.
"""
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from app.routes import health, chat

app = FastAPI(
    title="MLB Analytics Chatbot API",
    description="RAG-based baseball analytics chatbot backend",
    version="1.0.0"
)

# Configure CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://localhost:3000",
        "http://localhost:5173",  # Vite default port
        "http://127.0.0.1:3000",
        "http://127.0.0.1:5173",
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Register routes
app.include_router(health.router)
app.include_router(chat.router)


@app.get("/")
async def root():
    """Root endpoint."""
    return {
        "message": "MLB Analytics Chatbot API",
        "version": "1.0.0",
        "endpoints": {
            "health": "/health",
            "chat": "/chat"
        }
    }
