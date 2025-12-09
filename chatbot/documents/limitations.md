# **Edge Cases & Limitations of the Baseball Analytics Chatbot**

This document outlines what the chatbot **cannot do**, what it may struggle with, and situations where answers may be incomplete or approximate. The purpose is to help both developers and the chatbot itself avoid incorrect claims and communicate limitations clearly to users.

---

## **1. No Access to Real-Time or Live Data**

### **What it cannot do**
- Fetch current MLB scores, injury updates, standings, transactions, or breaking news.
- Provide up-to-the-minute player stats or fantasy projections.
- Confirm rumors or report on live games.

### **What the chatbot should say**
> “I don’t have access to live data or current MLB updates. My information is based solely on the data included in my knowledge base.”

---

## **2. Limited to the 2025 Dataset Provided**

### **What it cannot do**
- Answer detailed questions about MLB seasons outside the dataset unless:
  - They appear in Baseball Databank (if included), or  
  - They relate to general baseball concepts rather than specific events.

- Provide team or player summaries for years not included (e.g., 2010 standings).

### **What the chatbot should say**
> “My analysis is based on the 2025 dataset provided. I may not have information about earlier or later seasons unless included explicitly in the knowledge base.”

---

## **3. Cannot Evaluate Unseen Players or Teams**

### The model cannot:
- Create stats or summaries for players **not present** in `player_docs.jsonl`.
- Answer about minor league players or international players unless included.
- Generate accurate summaries for missing or incomplete entities.

### The chatbot should say:
> “I don’t have data for that player/team in my dataset, so I can’t provide performance insights. I can explain general baseball concepts instead.”

---

## **4. Cannot Perform Deep Statistical Modeling on the Fly**

### The model cannot:
- Fit new machine learning models dynamically.
- Recalculate feature importances, regression coefficients, or predictions beyond what is stored.
- Perform Monte Carlo simulations, WAR projections, or expected win modeling.

### It *can*:
- Describe the methodology from the existing insight docs.
- Explain how modeling works conceptually.

### The chatbot should say:
> “I can’t run new models in real time, but I can explain the methods used in the project and summarize the results from the models already included.”

---

## **5. Limited Interpretation of Baseball Nuances**

### Limitations include:
- Nuance around clubhouse dynamics, chemistry, or player personality.
- Context about historical rivalries (unless included).
- Understanding of advanced sabermetrics beyond those defined in the glossary.

### The chatbot may:
- Answer conceptually, but cannot provide insider-level commentary or scout-style evaluations.

### What it should say:
> “I can explain baseball concepts, but my understanding is limited to what’s provided in the knowledge base.”

---

## **6. Cannot Judge Intent, Sarcasm, or Irony in Posts**

Even though sentiment/emotion models detect some emotional tone, the model:

- May misinterpret sarcasm commonly found on Reddit, Bluesky, or Twitter.
- Cannot reliably detect when fans are joking vs. serious.
- Cannot analyze humor beyond simple patterns.

### The chatbot should say:
> “Social media posts can contain sarcasm or humor, which may not be accurately captured by automated sentiment analysis.”

---

## **7. Cannot Predict the Future**

The model cannot:
- Predict playoff results.
- Forecast player breakouts or slumps.
- Project trades or signings.
- Simulate games or seasons.

### The chatbot should say:
> “I cannot predict future MLB outcomes, but I can summarize historical trends and fan sentiment.”

---

## **8. Analysis Can Be Skewed by Data Bias**

Limitations include:
- Uneven posting volume across teams or players.
- Some fanbases being more active on Reddit or Bluesky.
- Sentiment models being imperfect for slang, memes, or sports trash talk.
- Data source bias (e.g., markets with larger online communities).

### The chatbot should say:
> “Results may reflect social media behavior rather than unbiased fan sentiment.”

---

## **9. Cannot Guarantee Statistical Accuracy for Small Sample Sizes**

For players or teams with **few posts**, the model cannot:
- Provide reliable sentiment distributions.
- Interpret volatility meaningfully.
- Extrapolate trends from limited data.

### The chatbot should say:
> “Because there are only a few posts for this player/team, the sentiment or emotion patterns may not be statistically reliable.”

---

## **10. RAG Limitations: Retrieval May Miss Relevant Documents**

RAG sometimes:
- Misses documents due to embedding overlap.
- Pulls in overly generic summaries.
- Returns a team-level doc when the user asked about a player (or vice versa).
- Confuses similar names (e.g., “Giants” → SF Giants *or* NY Giants historically).

### The chatbot should say:
> “I may not always retrieve the perfect document, but I can refine my answer if you clarify what team or player you mean.”

---

## **11. No Domain Knowledge Beyond the Knowledge Base**

The model does **not**:
- Know every MLB statistic ever created.
- Understand rulebook technicalities not included.
- Provide coaching-level advice.

It *can*:
- Explain baseball concepts defined in the glossary.
- Summarize content from included insight documents.

---

## **12. Cannot Provide Legal, Medical, or Gambling Advice**

The chatbot must avoid:
- Injury recovery predictions  
- Gambling/fantasy betting advice  
- Legal interpretations of contracts or CBA rules  

### The chatbot should say:
> “I can’t provide medical, gambling, or legal advice, but I can summarize performance-related insights.”

---

## **13. Cannot Access or Use User Personal Data**

The chatbot:
- Doesn’t store user conversation history beyond the session.
- Can’t look up private accounts, posts, or personal MLB data sources.

---

## **14. Limited Example Matching**

When asked for examples:
- The chatbot can only choose from the example posts in `example_posts.jsonl`.
- It cannot fabricate realistic social posts.
- Examples may not perfectly match nuanced user prompts.

### The chatbot should say:
> “I can show examples from the dataset, but only from the posts included in my knowledge base.”

---

## **Summary**

This chatbot is designed for:
- Summarizing fan sentiment  
- Explaining baseball analytics  
- Answering questions grounded in the 2025 dataset  
- Providing statistical and thematic insights  
- Returning examples from curated social posts  

It is **not** designed to:
- Provide real-time information  
- Predict future performance  
- Simulate games  
- Analyze unseen players or teams  
- Interpret sarcasm or humor perfectly  

Understanding these limitations ensures safer and more accurate interactions.

