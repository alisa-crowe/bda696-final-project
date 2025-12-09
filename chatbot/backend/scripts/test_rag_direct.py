#!/usr/bin/env python
"""
Direct RAG test script - tests the RAG pipeline without needing the API server.

Usage:
    python -m scripts.test_rag_direct
    python -m scripts.test_rag_direct "Which teams have the most positive fanbases?"
"""
import sys
from pathlib import Path

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app.rag.retrieval import retrieve, format_context
from app.rag.ollama_client import get_ollama_client
from app.rag.prompting import build_prompt, SYSTEM_PROMPT
from app.config import settings


def test_rag(query: str, show_context: bool = False):
    """Test the RAG pipeline directly."""
    print("\n" + "=" * 70)
    print(f"Query: {query}")
    print("=" * 70)
    
    # Step 1: Retrieve relevant documents
    print("\n[1/4] Retrieving relevant documents...")
    try:
        results = retrieve(query, n_results=6)
        print(f"   ✓ Found {len(results)} relevant documents")
        
        if show_context:
            print("\n   Retrieved documents:")
            for i, result in enumerate(results[:3], 1):  # Show top 3
                print(f"\n   {i}. Collection: {result.get('collection', 'unknown')}")
                print(f"      Doc Type: {result.get('doc_type', 'unknown')}")
                text_preview = result.get('text', '')[:150]
                print(f"      Preview: {text_preview}...")
                if result.get('team_name'):
                    print(f"      Team: {result['team_name']}")
                if result.get('player_name'):
                    print(f"      Player: {result['player_name']}")
    except Exception as e:
        print(f"   ✗ Error during retrieval: {e}")
        import traceback
        traceback.print_exc()
        return
    
    # Step 2: Format context
    print("\n[2/4] Formatting context...")
    try:
        context = format_context(results, max_chars=settings.MAX_CONTEXT_CHARS)
        context_length = len(context)
        print(f"   ✓ Context length: {context_length} characters")
        
        if show_context:
            print("\n   Context preview (first 500 chars):")
            print("   " + "-" * 66)
            print("   " + context[:500].replace("\n", "\n   "))
            print("   " + "-" * 66)
    except Exception as e:
        print(f"   ✗ Error formatting context: {e}")
        import traceback
        traceback.print_exc()
        return
    
    # Step 3: Build prompt
    print("\n[3/4] Building prompt...")
    try:
        prompt = build_prompt(context, query)
        prompt_length = len(prompt)
        print(f"   ✓ Prompt length: {prompt_length} characters")
    except Exception as e:
        print(f"   ✗ Error building prompt: {e}")
        import traceback
        traceback.print_exc()
        return
    
    # Step 4: Generate answer with Ollama
    print("\n[4/4] Generating answer with Ollama...")
    print(f"   Model: {settings.OLLAMA_MODEL_NAME}")
    print(f"   Base URL: {settings.OLLAMA_BASE_URL}")
    print("   (This may take 10-30 seconds...)")
    
    try:
        ollama_client = get_ollama_client()
        
        # Check connection first
        if not ollama_client.check_connection():
            print("   ✗ Error: Cannot connect to Ollama")
            print(f"      Make sure Ollama is running at {settings.OLLAMA_BASE_URL}")
            print("      Start it with: ollama serve")
            return
        
        answer = ollama_client.generate_answer(
            system_prompt=SYSTEM_PROMPT,
            context=context,
            user_query=query
        )
        
        print("\n" + "=" * 70)
        print("ANSWER:")
        print("=" * 70)
        print(answer)
        print("=" * 70)
        
        # Show sources
        if results:
            print(f"\nSources ({len(results)}):")
            for i, result in enumerate(results[:5], 1):  # Show top 5
                source_info = f"  {i}. [{result.get('collection', 'unknown')}]"
                if result.get('doc_type'):
                    source_info += f" {result['doc_type']}"
                if result.get('team_name'):
                    source_info += f" - Team: {result['team_name']}"
                if result.get('player_name'):
                    source_info += f" - Player: {result['player_name']}"
                print(source_info)
        
        return answer
        
    except Exception as e:
        print(f"   ✗ Error generating answer: {e}")
        import traceback
        traceback.print_exc()
        return


def interactive_mode():
    """Run in interactive mode."""
    print("\n" + "=" * 70)
    print("Interactive RAG Test Mode")
    print("=" * 70)
    print("Type your questions (or 'quit' to exit)")
    print("Commands:")
    print("  'show-context' - Toggle showing retrieved context")
    print("  'quit' or 'exit' - Exit")
    print()
    
    show_context = False
    
    while True:
        try:
            query = input("You: ").strip()
            if not query:
                continue
            
            if query.lower() in ['quit', 'exit', 'q']:
                break
            
            if query.lower() == 'show-context':
                show_context = not show_context
                print(f"   {'Showing' if show_context else 'Hiding'} context in future queries")
                continue
            
            test_rag(query, show_context=show_context)
            print()
            
        except KeyboardInterrupt:
            print("\n\nExiting...")
            break
        except Exception as e:
            print(f"Error: {e}")
            import traceback
            traceback.print_exc()


def main():
    """Main test function."""
    print("MLB Analytics Chatbot - Direct RAG Test")
    print("=" * 70)
    print(f"Ollama Model: {settings.OLLAMA_MODEL_NAME}")
    print(f"Embedding Provider: {settings.EMBEDDING_PROVIDER}")
    print(f"Embedding Model: {settings.EMBEDDING_MODEL_NAME}")
    print("=" * 70)
    
    # Check Ollama connection
    print("\nChecking Ollama connection...")
    try:
        ollama_client = get_ollama_client()
        if ollama_client.check_connection():
            print("✓ Ollama is connected")
        else:
            print("✗ Ollama is not connected")
            print(f"  Make sure Ollama is running at {settings.OLLAMA_BASE_URL}")
            print("  Start it with: ollama serve")
            sys.exit(1)
    except Exception as e:
        print(f"✗ Error checking Ollama: {e}")
        sys.exit(1)
    
    # Check if query provided as argument
    if len(sys.argv) > 1:
        query = " ".join(sys.argv[1:])
        test_rag(query, show_context=False)
    else:
        # Run example queries
        example_queries = [
            "Which teams have the most positive fanbases?",
            "What is WAR?",
            "Tell me about the Atlanta Braves",
            "How are sentiment scores calculated?",
        ]
        
        print("\n" + "=" * 70)
        print("Running Example Queries")
        print("=" * 70)
        
        for i, query in enumerate(example_queries, 1):
            print(f"\nExample {i}/{len(example_queries)}")
            test_rag(query, show_context=False)
            print()
        
        # Ask if user wants interactive mode
        print("\n" + "=" * 70)
        response = input("Run interactive mode? (y/n): ").strip().lower()
        if response == 'y':
            interactive_mode()


if __name__ == "__main__":
    main()
