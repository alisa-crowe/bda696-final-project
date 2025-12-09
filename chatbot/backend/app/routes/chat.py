"""
Chat endpoint for RAG-based question answering.
"""
from fastapi import APIRouter, HTTPException
from app.models.schemas import ChatRequest, ChatResponse, Source
from app.rag.retrieval import retrieve, format_context
from app.rag.prompting import SYSTEM_PROMPT
from app.rag.ollama_client import get_ollama_client

router = APIRouter()


@router.post("/chat", response_model=ChatResponse)
async def chat(request: ChatRequest):
    """
    Main chat endpoint.
    Retrieves relevant context and generates an answer using Ollama.
    """
    try:
        # Retrieve relevant documents
        results = retrieve(request.message)

        if not results:
            return ChatResponse(
                answer="I couldn't find relevant information in my knowledge base to answer your question. "
                       "Could you rephrase it or ask about something else?",
                sources=None
            )

        # Format context
        context = format_context(results)

        # Generate answer
        ollama_client = get_ollama_client()
        answer = ollama_client.generate_answer(
            system_prompt=SYSTEM_PROMPT,
            context=context,
            user_query=request.message,
            history=request.history
        )

        # Format sources
        sources = []
        for result in results[:10]:  # Top 10 sources
            metadata = result.get("metadata", {})
            sources.append(Source(
                collection=result.get("collection", "unknown"),
                doc_type=metadata.get("doc_type", "unknown"),
                source_file=metadata.get("source_file"),
                team_name=metadata.get("team_name"),
                player_name=metadata.get("player_name"),
                section=metadata.get("section")
            ))

        return ChatResponse(
            answer=answer,
            sources=sources
        )

    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Error processing chat request: {str(e)}"
        )
