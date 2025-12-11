#!/usr/bin/env python
"""
CLI script to build or rebuild the Chroma index from documents.

Usage:
    python -m scripts.build_index [--rebuild]
"""
import argparse
import sys
from pathlib import Path

# Add parent directory to path so we can import app modules
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app.rag.indexing import index_all_documents
from app.config import settings


def main():
    parser = argparse.ArgumentParser(
        description="Build or rebuild Chroma index from documents"
    )
    parser.add_argument(
        "--rebuild",
        action="store_true",
        help="Delete existing collections before indexing"
    )
    args = parser.parse_args()

    print("=" * 60)
    print("Building Chroma Index")
    print("=" * 60)
    print(f"Documents directory: {settings.DOCUMENTS_DIR}")
    print(f"Chroma DB directory: {settings.CHROMA_DB_DIR}")
    print(f"Rebuild mode: {args.rebuild}")
    print()

    try:
        stats = index_all_documents(rebuild=args.rebuild)
        print()
        print("=" * 60)
        print("Indexing Complete")
        print("=" * 60)
        for collection, count in stats.items():
            print(f"  {collection}: {count} documents")
        print()
        print("Total documents indexed:", sum(stats.values()))
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
