#!/usr/bin/env python
"""
Test script to verify retrieval improvements for teams and players.
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app.rag.retrieval import retrieve, route_query
from app.config import settings


def test_team_retrieval():
    """Test team retrieval."""
    print("=" * 70)
    print("Testing Team Retrieval")
    print("=" * 70)
    
    queries = [
        "Tell me about the Atlanta Braves",
        "What are the stats for the Yankees?",
        "How did the Dodgers perform?",
    ]
    
    for query in queries:
        print(f"\nQuery: {query}")
        print("-" * 70)
        
        # Test routing
        collections, filters = route_query(query)
        print(f"Collections: {collections}")
        print(f"Filters: {filters}")
        
        # Test retrieval
        results = retrieve(query, n_results=5)
        
        print(f"\nRetrieved {len(results)} documents:")
        for i, result in enumerate(results[:5], 1):
            print(f"\n  {i}. Collection: {result.get('collection')}")
            print(f"     Team: {result.get('metadata', {}).get('team_name', 'N/A')}")
            print(f"     Distance: {result.get('distance', 'N/A'):.4f}")
            text_preview = result.get('text', '')[:100].replace('\n', ' ')
            print(f"     Preview: {text_preview}...")


def test_player_retrieval():
    """Test player retrieval."""
    print("\n" + "=" * 70)
    print("Testing Player Retrieval")
    print("=" * 70)
    
    queries = [
        "Tell me about Mike Trout",
        "What are Aaron Judge's stats?",
        "How did Shohei Ohtani perform?",
    ]
    
    for query in queries:
        print(f"\nQuery: {query}")
        print("-" * 70)
        
        # Test routing
        collections, filters = route_query(query)
        print(f"Collections: {collections}")
        print(f"Filters: {filters}")
        
        # Test retrieval
        results = retrieve(query, n_results=5)
        
        print(f"\nRetrieved {len(results)} documents:")
        for i, result in enumerate(results[:5], 1):
            print(f"\n  {i}. Collection: {result.get('collection')}")
            print(f"     Player: {result.get('metadata', {}).get('player_name', 'N/A')}")
            print(f"     Distance: {result.get('distance', 'N/A'):.4f}")
            text_preview = result.get('text', '')[:100].replace('\n', ' ')
            print(f"     Preview: {text_preview}...")


if __name__ == "__main__":
    test_team_retrieval()
    test_player_retrieval()
