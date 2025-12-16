# Beyond the Box Score: Discovering What Really Wins Baseball Games

Final project for **BDA 696: Advanced Special Topics in Big Data Analytics**  
San Diego State University  
Instructor: Ryan Lafler

**Authors:** Alisa Crowe, Rita Herfi, Samantha Keppler

## Overview

This project investigates which statistics most directly contribute to team success in Major League Baseball by analyzing player and game-level metrics, fan sentiment from social media, and their relationships to team performance outcomes. The analysis combines structured numerical data from the Baseball Databank with unstructured textual data scraped from Reddit and Bluesky to explore how offensive, defensive, and pitching statistics interact to predict team wins, and whether fan sentiment reflects real-life performance outcomes.

## Project Structure

- **team-data/**: Social media data collection and processing for team-level analysis
  - Scrapers for Reddit and Bluesky posts
  - Data cleaning and team name standardization scripts
  - Combined team datasets with performance statistics

- **player-data/**: Player-level data collection and processing
  - Reddit post scraping for individual players
  - Player statistics integration and matching

- **text-mining/**: Sentiment analysis, emotion classification, and thematic labeling
  - VADER sentiment scoring
  - NRC Emotion Lexicon classification
  - Keyword-based theme extraction

- **success-factors/**: Predictive modeling for team success
  - Multiple linear regression for win percentage prediction
  - Logistic regression for playoff qualification prediction
  - Feature importance analysis

- **chatbot/**: Interactive RAG-based chatbot for querying analysis results
  - FastAPI backend with ChromaDB vector database
  - React frontend interface
  - Retrieval-augmented generation using Ollama LLM

- **baseball-databank/**: Historical MLB statistics (1871-2015) from Kaggle

## Key Technologies

- **Data Collection**: PRAW (Reddit API), atproto (Bluesky API), BeautifulSoup
- **Data Processing**: pandas, NumPy
- **Text Analysis**: NLTK (VADER), NRC Emotion Lexicon, transformers
- **Modeling**: scikit-learn (linear/logistic regression)
- **Visualization**: matplotlib, Tableau
- **Chatbot**: FastAPI, ChromaDB, Ollama, React, TypeScript

## Setup

### Prerequisites

- Python 3.9+
- Node.js 18+ (for chatbot frontend)
- Ollama installed and running (for chatbot LLM)

### Installation

1. Install Python dependencies:
```bash
pip install -r requirements.txt
```

2. For the chatbot, install backend dependencies:
```bash
cd chatbot/backend
pip install -r requirements.txt
```

3. Build the chatbot knowledge base index:
```bash
cd chatbot/backend
python -m scripts.build_index --rebuild
```

4. Install frontend dependencies:
```bash
cd chatbot/frontend
npm install
```

See `chatbot/QUICKSTART.md` for detailed chatbot setup instructions.

## Data Sources

- **Baseball Databank**: Historical MLB statistics from Kaggle (1871-2015)
- **Social Media**: Reddit and Bluesky posts about MLB teams and players (2025 season)
- **Team Statistics**: 2025 MLB regular season performance metrics

## Key Findings

- Defensive and pitching metrics (particularly runs allowed) are stronger predictors of team success than offensive metrics
- Fan sentiment correlates with team performance, with playoff teams showing higher sentiment scores
- Emotion analysis provides richer signals than scalar sentiment scores for differentiating team performance
- Fanbase volatility profiles reveal distinct behavioral patterns independent of team performance

## Paper

See "Final Paper.pdf" for the complete research paper documenting methodology, results, and conclusions.

