"""
FastAPI application entrypoint.
"""
from fastapi import FastAPI
from app.routes import health, chat

app = FastAPI(
    title="MLB Analytics Chatbot API",
    description="RAG-based baseball analytics chatbot backend",
    version="1.0.0"
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
