# Retrieval Fixes for Team and Player Queries

## Problem

The chatbot was not retrieving team and player documents correctly. When asking "Tell me about the Atlanta Braves", it would return generic insights instead of the detailed team summary with stats.

## Root Causes

1. **No metadata filtering**: The system was using only semantic similarity search, which didn't prioritize exact team/player matches
2. **No team name normalization**: Keywords like "braves" or "atlanta" weren't mapped to full team names like "Atlanta Braves"
3. **Collection prioritization**: Teams/players collections weren't prioritized when those entities were detected
4. **No exact match boosting**: Exact matches weren't boosted in the results

## Fixes Applied

### 1. Team Name Mapping
- Added `TEAM_NAME_MAP` dictionary that maps keywords/abbreviations to full team names
- Example: "braves" → "Atlanta Braves", "nyy" → "New York Yankees"

### 2. Metadata Filtering
- When a team is detected in the query, the system now filters the `teams` collection by `team_name`
- When a player is detected, it filters the `players` collection by `player_name`
- Uses Chroma's `where` clause for exact metadata matching

### 3. Collection Prioritization
- Teams collection is placed first when a team is detected
- Players collection is placed first when a player is detected
- Increases number of results from primary collections (3x when filtering)

### 4. Exact Match Boosting
- Exact team/player matches get a distance boost (0.5 reduction)
- Results are sorted by adjusted distance, putting exact matches first

### 5. Fallback Mechanism
- If filtering returns no results, falls back to semantic search without filter
- Ensures we always get some results even if metadata doesn't match exactly

## Testing

Run the test script to verify:

```bash
cd chatbot/backend
python -m scripts.test_retrieval
```

Or test with the chatbot:

```bash
# Test team query
python -m scripts.test_rag_direct "Tell me about the Atlanta Braves"

# Test player query  
python -m scripts.test_rag_direct "Tell me about Mike Trout"
```

## Expected Behavior

### Before Fix
- Query: "Tell me about the Atlanta Braves"
- Results: Generic insights about joy in successful teams
- Missing: Actual Braves stats (wins, runs, sentiment scores, etc.)

### After Fix
- Query: "Tell me about the Atlanta Braves"
- Results: 
  1. **Atlanta Braves team summary** (exact match, boosted)
  2. Fan posts about Braves
  3. Global insights mentioning Braves
- Includes: All stats from team_docs.jsonl (97 wins, 0.599 win%, 67.7% positive sentiment, etc.)

## Files Modified

- `app/rag/retrieval.py`: 
  - Added `TEAM_NAME_MAP` dictionary
  - Improved `route_query()` to normalize team names and set filters
  - Enhanced `retrieve()` to use metadata filters and boost exact matches
  - Added fallback mechanism

## No Re-indexing Required

The fixes are in the retrieval logic only. The existing index is fine - we just needed to query it more intelligently.

## Next Steps

1. Test the fixes with various team and player queries
2. Monitor retrieval quality - exact matches should appear first
3. If issues persist, check:
   - Team/player names in metadata match exactly (case-sensitive)
   - Chroma version supports the `where` clause syntax used
