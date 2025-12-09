# **Baseball Analytics Chatbot – Data Dictionary & Glossary**

Alphabetized glossary of all baseball statistics, sentiment metrics, emotion labels, and classification themes used in the chatbot’s RAG knowledge base.

---

## **A**

### **AVG (Batting Average)**
A hitting metric showing how often a player records a hit.  
\[
AVG = \frac{\text{Hits}}{\text{At-Bats}}
\]  
Higher values indicate better offensive performance.

### **Anger (Emotion Label)**
Emotion classification indicating frustration, hostility, or outrage in fan posts.

---

## **B**

### **BABIP (Batting Average on Balls in Play)** *(if applicable)*  
Measures how often a ball in play becomes a hit. Useful for evaluating luck or defensive play.

### **BB/9 (Walks per 9 Innings)**
Pitching stat measuring how many walks a pitcher allows per nine innings pitched. Lower is better.

### **Batting Average (AVG)**
See **AVG**.

### **BB (Walk / Base on Balls)**
Occurs when a batter receives four balls and advances to first base.

### **BBE (Batted Ball Event)** *(if applicable)*  
Any ball hit into the field of play.

---

## **C**

### **Char_len**
Number of characters in a social post. Useful for analyzing post length but not sentiment.

### **Compound Sentiment Score (VADER)**
Raw VADER sentiment score ranging from **-1 (very negative)** to **+1 (very positive)**.

---

## **D**

### **Disgust (Emotion Label)**
Represents revulsion, annoyance, or contempt in fan reactions.

### **Doubles (2B)**
A hit where the batter reaches second base.

---

## **E**

### **Emotion Label**
A categorical value assigned by the emotion classifier (DistilRoBERTa).  
Possible labels:
- anger  
- disgust  
- fear  
- joy  
- sadness  
- surprise  
- neutral  

### **ERA (Earned Run Average)**
Pitching metric estimating earned runs allowed per 9 innings:  
\[
ERA = 9 \times \frac{\text{Earned Runs Allowed}}{\text{Innings Pitched}}
\]  
Lower is better.

---

## **F**

### **Fan Experience (Theme Label)**
Posts discussing:
- attending games  
- stadium atmosphere  
- fan rituals  
- emotions associated with fandom  

### **Fear (Emotion Label)**
Represents worry, anxiety, or uncertainty in fan posts.

---

## **G**

### **GM (General Manager)**
Executive responsible for player transactions and roster construction.

---

## **H**

### **HR (Home Runs)**
Balls hit out of the park in fair territory.

### **Humor & Memes (Theme Label)**
Posts meant to be humorous, sarcastic, or meme-driven.

---

## **I**

### **ISO (Isolated Power)** *(if applicable)*  
\[
ISO = SLG - AVG
\]  
Measures a hitter’s raw power.

---

## **J**

### **Joy (Emotion Label)**
Represents happiness, excitement, or positivity in fan posts.

---

## **K**

### **K/9 (Strikeouts per 9 Innings)**
Pitching stat showing strikeouts per nine innings. Higher is better.

---

## **L**

### **Losses (team_losses)**
Total games a team (or pitcher) has lost.

### **League Overview**
A global summary of league-wide sentiment, themes, and performance.

---

## **M**

### **Management Decisions (Theme Label)**
Posts about:
- coaching choices  
- front-office moves  
- trades  
- roster construction  
- strategic decisions  

Often associated with strong emotions (anger, frustration).

### **Make Playoffs (team_make_playoffs)**
Indicates postseason qualification.

---

## **N**

### **Neutral (Emotion or Sentiment Label)**
- As sentiment: neither clearly positive nor negative.  
- As emotion: calm, factual, and non-reactive.

---

## **O**

### **OBP (On-base Percentage)**
\[
OBP = \frac{Hits + Walks + Hit\:By\:Pitch}{At-Bats + Walks + Hit\:By\:Pitch + Sacrifice\:Flies}
\]

### **Offseason & Future Outlook (Theme Label)**
Topics include:
- predictions  
- prospects  
- long-term planning  

### **Off-topic or Cross-sport (Theme Label)**
Posts unrelated to baseball or referencing other sports.

### **OPS (On-base Plus Slugging)**
\[
OPS = OBP + SLG
\]  
Measures a hitter's ability to get on base and hit for power.

---

## **P**

### **Player Performance (Theme Label)**
Posts discussing:
- player stats  
- injuries  
- hot streaks / slumps  
- overall evaluation  

### **Playoff Odds / Predictions**
Model-based or fan-based predictions of postseason chances.

---

## **Q**

*(No common baseball analytics terms beginning with Q in this dataset.)*

---

## **R**

### **RBI (Runs Batted In)**
Credit awarded when a batter drives in a run.

### **Run Differential (team_run_diff)**
\[
Run\:Diff = Runs\:Scored - Runs\:Allowed
\]  
Strong predictor of team quality.

---

## **S**

### **Sadness (Emotion Label)**
Represents disappointment or discouragement.

### **SLG (Slugging Percentage)**
\[
SLG = \frac{Total\:Bases}{At-Bats}
\]  
Measures hitting power by weighting extra-base hits.

### **Sentiment Label**
One of:
- positive  
- neutral  
- negative  

### **Sentiment Score**
Numeric VADER score ranging from -1 to +1.

### **Sentiment Volatility (sentiment_std)**
Standard deviation of sentiment scores:
- **High volatility** → fans swing between emotions  
- **Low volatility** → stable sentiment

### **Surprise (Emotion Label)**
Represents shock, amazement, or unexpected developments.

---

## **T**

### **Team Performance (Theme Label)**
Posts discussing:
- wins/losses  
- standings  
- pitching/hitting evaluations  
- game results  

### **Team Payroll (team_TeamPayroll)**
Total salary commitments for the team.

---

## **U**

### **Underrated / Overrated**
Comparison between sentiment and performance metrics:
- **Underrated** = poor sentiment + strong performance  
- **Overrated** = strong sentiment + weak performance  

---

## **V**

### **VADER (Sentiment Model)**
Rule-based NLP model used to compute sentiment scores.

### **Volatility (Sentiment)**
See **Sentiment Volatility**.

---

## **W**

### **WAR (Wins Above Replacement)**
Estimate of how many additional wins a player contributes compared to a replacement-level player.

### **WHIP (Walks + Hits per Inning Pitched)**
\[
WHIP = \frac{Walks + Hits}{Innings\:Pitched}
\]  
Lower values indicate better pitching control.

### **Win Percentage (team_win_pct)**
\[
Win\% = \frac{Wins}{Wins + Losses}
\]

---

## **X**
*(Reserved for future terms.)*

---

## **Y**

### **Year (Season)**
Season associated with a post or performance statistic (e.g., 2025).

---

## **Z**
*(Reserved for future entries.)*
