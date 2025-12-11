# Methodology and Definitions

This document explains **how the data was processed** and **what each metric means** for the baseball analytics chatbot. It is meant to be a reference both for humans and for the chatbot’s RAG knowledge base.

---

## 1. Data Sources

We use three main data sources:

1. **Social + Performance (Teams & Players)**  
   - `combined_team_and_player_data.csv`  
   - Integrated dataset combining:
     - Social posts about MLB teams and players
     - Team-level and player-level performance metrics
     - Basic metadata (source, author, permalink, timestamp)

2. **Team Social Sentiment & Themes**  
   - `team_data_with_themes_final.csv`  
   - One row per social post about a team, with:
     - Team performance metrics (prefixed with `team_`)
     - Sentiment scores and labels
     - Emotion labels
     - Theme labels

3. **Baseball Databank (optional / future)**  
   - Historical MLB stats and metadata for teams and players
   - Can be used for historical context and comparisons, but is not currently the main focus.

Time period: **2025 season** (plus surrounding postseason / playoff conversation where available).

---

## 2. Social Metrics

### 2.1 Sentiment Scores

- **Model**: VADER (Valence Aware Dictionary for Sentiment Reasoning)
- **Field**: `sentiment_score`
- **Range**: approximately from -1 (very negative) to +1 (very positive)
- **How it’s computed**:
  - For each post `text`, we compute the VADER *compound* sentiment score.
  - This becomes `sentiment_score` in the dataset.
  - We also bucket into a **sentiment label**:
    - `positive`
    - `neutral`
    - `negative`

**Interpretation**:
- Values near **0** ≈ mixed or neutral sentiment.
- Values > 0.3 (rule-of-thumb) often behave like “positive”.
- Values < -0.3 often behave like “negative”.

When aggregating (e.g., per team or per player), we report:
- `sentiment_avg`: average sentiment score across posts
- `sentiment_std`: standard deviation (volatility of sentiment over time)

---

### 2.2 Emotion Classification

- **Model**: `j-hartmann/emotion-english-distilroberta-base`
- **Field**: `emotion_label`
- **Labels** (typical):
  - `anger`
  - `disgust`
  - `fear`
  - `joy`
  - `sadness`
  - `surprise`
  - `neutral`

For each post:
- We run the emotion model on the `text`.
- The most likely emotion becomes `emotion_label`.

When aggregating:
- We compute the **distribution** of emotion labels, e.g.:
  - `neutral (68.0%), joy (15.0%), anger (10.0%), sadness (7.0%)`

---

### 2.3 Theme Classification

- **Model**: Zero-shot classification using a BART-based model
- **Field**: `theme_label`
- **Candidate themes** (examples):
  - `player performance`
  - `team performance`
  - `management decisions`
  - `fan experience`
  - `offseason & future outlook`
  - `media & analysis`
  - `off-topic or cross-sport`
  - `humor & memes`
  - `baseball culture & nostalgia`

For each post:
- We run zero-shot classification with the list of candidate labels.
- The highest-scoring theme becomes `theme_label`.

When aggregating:
- We compute the **distribution** of themes per team or player, e.g.:
  - `player performance (55%), fan experience (20%), management decisions (15%), media & analysis (10%)`.

---

## 3. Performance Metrics

### 3.1 Team-Level Metrics

All team performance columns are prefixed with `team_` in the themes dataset:

- `team_win_pct`: **Win Percentage**  
  \[
  \text{Win\%} = \frac{\text{Wins}}{\text{Wins} + \text{Losses}}
  \]
- `team_wins`, `team_losses`: total wins / losses.
- `team_runs_scored`, `team_runs_allowed`: runs for and against.
- `team_run_diff`:
  \[
  \text{Run Diff} = \text{Runs Scored} - \text{Runs Allowed}
  \]
- `team_ERA`: **Earned Run Average** (team pitching)
- `team_WHIP`: **Walks + Hits per Inning Pitched**
- `team_OBP`: On-base percentage
- `team_SLG`: Slugging percentage
- `team_OPS`: On-base + slugging
- `team_HR`: Team home runs
- `team_K_per_9`, `team_BB_per_9`: strikeouts and walks per 9 innings
- `team_TeamPayroll`: team payroll for the season
- `team_make_playoffs`: indicator for playoff appearance (`Yes` / `No`)

When we build **team summary docs**, we typically report:
- Average `team_win_pct` across posts
- Average `team_runs_scored` and `team_runs_allowed`
- Approximate record from `team_wins` and `team_losses`

> Note: Because performance metrics are joined at the post level, multiple posts may repeat the same season-level stats. We handle this by averaging within each team/season group.

---

### 3.2 Player-Level Metrics

Player metrics (in `combined_team_and_player_data.csv`) follow a similar pattern (exact column names may differ, but generally include):

Batting:
- `player_AVG`: Batting average
- `player_HR`: Home runs
- `player_RBI`: Runs batted in
- `player_OBP`: On-base percentage
- `player_SLG`: Slugging percentage
- `player_OPS`: On-base plus slugging

Pitching:
- `player_ERA`: Earned run average
- `player_WHIP`: Walks + hits per inning pitched
- `player_K_per_9`: Strikeouts per 9 innings
- `player_BB_per_9`: Walks per 9 innings

When we build **player summary docs**, we report:
- Mean batting or pitching stats across rows for that player (per team/season)
- Number of associated posts
- Sentiment/emotion/theme patterns for that player (if available)

---

## 4. Modeling Overview (High-Level)

The project includes several modeling components. At a high level:

1. **Predicting Team Success from Performance Metrics**
   - Goal: predict a team outcome (e.g., playoff appearance or high win percentage) from season-level stats.
   - Input features: `team_win_pct`, `team_run_diff`, `team_OPS`, `team_ERA`, `team_WHIP`, etc.
   - Model type: tree-based model (e.g., Random Forest, Gradient Boosting) or logistic regression.
   - Evaluation: ROC AUC and accuracy (e.g., ROC AUC ~ X.XX).

2. **Sentiment vs. Performance Analysis**
   - Goal: understand how fan sentiment relates to on-field results.
   - Approach:
     - Aggregate sentiment scores per team/season.
     - Correlate `sentiment_avg` with `team_win_pct`, `team_run_diff`, etc.
   - Interpretation: positive correlations suggest that better-performing teams tend to get more positive sentiment.

3. **Volatility and Emotion Patterns**
   - We use `sentiment_std` to capture how “volatile” the conversation is:
     - High std = emotional swings / polarized discussion.
     - Low std = more stable sentiment.
   - Emotion distributions (`emotion_label`) show whether a fanbase leans more toward `joy`, `anger`, `sadness`, etc.

---

## 5. How the Chatbot Uses These Documents

The RAG pipeline uses several types of documents:

1. **Team Summary Docs (`team_docs.jsonl`)**
   - One doc per team (and optionally season).
   - Contains:
     - Aggregated team performance metrics.
     - Sentiment / emotion / theme distributions.
     - A short natural-language summary.

2. **Player Summary Docs (`player_docs.jsonl`)**
   - One doc per player (optionally per team/season).
   - Contains:
     - Key performance metrics.
     - Sentiment/emotion/theme patterns around that player.
     - A short natural-language summary.

3. **Insight Documents (Markdown)**
   - Global insights about:
     - Feature importance for predictive models
     - Sentiment vs. win percentage
     - Emotions by team
     - Thematic patterns and volatility

4. **Methodology / Definitions (this document)**
   - Helps the chatbot explain:
     - Where numbers come from
     - What metrics mean
     - How to interpret model outputs

When a user asks a question, the chatbot:
- Retrieves the most relevant team / player / insight docs.
- Uses these summaries as **grounding context**.
- Generates an answer that is consistent with the data and methodology described here.

---
