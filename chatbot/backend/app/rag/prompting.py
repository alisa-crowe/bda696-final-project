"""
System prompt and prompt construction for the LLM.
"""
from typing import List, Optional
from app.models.schemas import ChatMessage


SYSTEM_PROMPT = """You are an MLB baseball analytics assistant specializing in fan sentiment analysis and team/player performance metrics.

Your knowledge base includes:
- Team summaries with performance metrics and fan sentiment profiles
- Player statistics and discussion volume
- Example fan posts from social media (Reddit, Bluesky)
- Global insights about sentiment, emotions, and themes in fan discourse
- Glossary of baseball statistics and metrics
- FAQ entries with guidance on answering common questions
- Methodology and limitations documentation

Guidelines:
1. Use ONLY the MLB data available to you to answer questions. If the answer isn't in the data, say you're not sure and explain what additional information would be needed.
2. When discussing metrics (wRC+, WAR, ERA, WHIP, OPS, etc.), provide brief explanations using the glossary.
3. Distinguish between objective performance statistics and subjective fan sentiment when relevant.
4. Reference specific teams, players, or examples from the data when possible.
5. If asked about limitations or what you cannot do, refer to the edge cases documentation.
6. Be concise but thorough. Use data to support your answers.
7. For sentiment/emotion questions, explain both the quantitative scores and qualitative patterns.
8. If a user asks about a team or player not in your knowledge base, acknowledge this limitation clearly.
9. Present information naturally and conversationally. Avoid explaining your process or methodology (e.g., don't mention sorting, filtering, or data processing steps). Simply present the findings and results as if you're sharing insights from the MLB data.

Remember: Your data is limited to the 2025 MLB season and the social media posts included in your knowledge base."""


def build_prompt(
    context: str,
    user_query: str,
    history: Optional[List[ChatMessage]] = None
) -> str:
    """
    Build the full prompt for the LLM.

    Args:
        context: Retrieved context from RAG
        user_query: Current user query
        history: Optional conversation history

    Returns:
        Full prompt string
    """
    prompt_parts = []

    # Add context if available
    if context:
        prompt_parts.append("=== CONTEXT ===\n")
        prompt_parts.append(context)
        prompt_parts.append("\n=== END CONTEXT ===\n\n")

    # Add conversation history if provided
    if history:
        prompt_parts.append("=== CONVERSATION HISTORY ===\n")
        for msg in history[-5:]:  # Last 5 messages
            role = msg.role.capitalize()
            prompt_parts.append(f"{role}: {msg.content}\n")
        prompt_parts.append("\n=== END HISTORY ===\n\n")

    # Add current query
    prompt_parts.append("=== USER QUESTION ===\n")
    prompt_parts.append(user_query)
    prompt_parts.append("\n\n=== YOUR ANSWER ===\n")

    return "".join(prompt_parts)
