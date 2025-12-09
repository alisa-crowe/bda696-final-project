#!/usr/bin/env python
"""
Interactive test script for the chat API.

Usage:
    python -m scripts.test_chat
"""
import sys
import json
from pathlib import Path

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

try:
    import requests
except ImportError:
    print("Error: requests not installed. Install with: pip install requests")
    sys.exit(1)


BASE_URL = "http://localhost:8000"


def test_health():
    """Test the health endpoint."""
    print("=" * 60)
    print("Testing /health endpoint...")
    print("=" * 60)
    try:
        response = requests.get(f"{BASE_URL}/health", timeout=5)
        response.raise_for_status()
        data = response.json()
        print(json.dumps(data, indent=2))
        
        if data.get("status") == "healthy":
            print("\n✅ Health check passed!")
            return True
        else:
            print(f"\n⚠️  Status: {data.get('status')}")
            return False
    except requests.exceptions.ConnectionError:
        print("❌ Error: Could not connect to API.")
        print(f"   Make sure the API is running at {BASE_URL}")
        print("   Start it with: uvicorn app.main:app --reload --port 8000")
        return False
    except Exception as e:
        print(f"❌ Error: {e}")
        return False


def test_chat(message: str, history=None):
    """Test the chat endpoint."""
    print("\n" + "=" * 60)
    print(f"Testing /chat endpoint...")
    print("=" * 60)
    print(f"Query: {message}")
    print()
    
    try:
        payload = {"message": message}
        if history:
            payload["history"] = history
        
        response = requests.post(
            f"{BASE_URL}/chat",
            json=payload,
            timeout=120  # LLM generation can take time
        )
        response.raise_for_status()
        data = response.json()
        
        print("Answer:")
        print("-" * 60)
        print(data.get("answer", "No answer returned"))
        print("-" * 60)
        
        if data.get("sources"):
            print(f"\nSources ({len(data['sources'])}):")
            for i, source in enumerate(data["sources"][:5], 1):  # Show top 5
                print(f"  {i}. {source.get('collection')} - {source.get('doc_type')}")
                if source.get("team_name"):
                    print(f"     Team: {source['team_name']}")
                if source.get("player_name"):
                    print(f"     Player: {source['player_name']}")
        
        return True
    except requests.exceptions.Timeout:
        print("❌ Error: Request timed out. The LLM might be taking too long.")
        return False
    except Exception as e:
        print(f"❌ Error: {e}")
        if hasattr(e, 'response') and e.response is not None:
            try:
                error_data = e.response.json()
                print(f"   Details: {json.dumps(error_data, indent=2)}")
            except:
                print(f"   Response: {e.response.text}")
        return False


def interactive_mode():
    """Run in interactive mode."""
    print("\n" + "=" * 60)
    print("Interactive Chat Mode")
    print("=" * 60)
    print("Type your questions (or 'quit' to exit)")
    print()
    
    history = []
    
    while True:
        try:
            query = input("You: ").strip()
            if not query:
                continue
            if query.lower() in ['quit', 'exit', 'q']:
                break
            
            # Test chat
            success = test_chat(query, history if history else None)
            
            if success:
                # Add to history (simplified - in real app, you'd get the actual response)
                history.append({"role": "user", "content": query})
                # Note: In a real app, you'd add the assistant's response too
            
            print()
            
        except KeyboardInterrupt:
            print("\n\nExiting...")
            break
        except Exception as e:
            print(f"Error: {e}")


def main():
    """Main test function."""
    print("MLB Analytics Chatbot - API Test Script")
    print("=" * 60)
    
    # Test health first
    if not test_health():
        print("\n⚠️  Health check failed. Please fix issues before testing chat.")
        sys.exit(1)
    
    # Check if running in interactive mode or with arguments
    if len(sys.argv) > 1:
        # Test with provided query
        query = " ".join(sys.argv[1:])
        test_chat(query)
    else:
        # Run example queries
        example_queries = [
            "Which teams have the most positive fanbases?",
            "What is WAR?",
            "Tell me about the Atlanta Braves",
            "How are sentiment scores calculated?",
        ]
        
        print("\n" + "=" * 60)
        print("Running Example Queries")
        print("=" * 60)
        
        for i, query in enumerate(example_queries, 1):
            print(f"\nExample {i}/{len(example_queries)}")
            test_chat(query)
            print()
        
        # Ask if user wants interactive mode
        print("\n" + "=" * 60)
        response = input("Run interactive mode? (y/n): ").strip().lower()
        if response == 'y':
            interactive_mode()


if __name__ == "__main__":
    main()
