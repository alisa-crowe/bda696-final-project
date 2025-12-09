# Insight: Fanbase Volatility and Sentiment Profiles

This analysis plots each team by two dimensions of fan discourse:
- **Average sentiment** (horizontal axis)  
- **Sentiment volatility (standard deviation)** (vertical axis)

The combination reveals distinct fanbase personality types that are not explained by team performance alone. Playoff and non-playoff teams are also distinguished.

## Key Findings

### 1. Four Distinct Fanbase Profiles Emerge

#### A. Joyful Diehards (High Sentiment, High Volatility)
These fanbases display:
- consistently **positive sentiment**, and  
- **emotionally reactive** posting patterns.

Examples include:
- **Milwaukee Brewers (MIL)**
- **Seattle Mariners (SEA)**
- **Los Angeles Dodgers (LAD)**

Interpretation:
- These fans are passionate and highly expressive.
- Volatility reflects rapid emotional swings during wins, losses, or key moments.

---

#### B. Steady Optimists (High Sentiment, Low Volatility)
Characteristics:
- Positive but **stable**, with fewer emotional extremes.
- Optimistic regardless of short-term fluctuations.

Examples:
- **Atlanta Braves (ATL)** — highest sentiment in the dataset.  
- **Toronto Blue Jays (TOR)**  
- **Minnesota Twins (MIN)** (mid-high sentiment, low volatility)

Interpretation:
- These fans maintain confidence in their team.
- Sentiment is not easily disrupted by temporary setbacks.

---

#### C. Emotional Pessimists (Low Sentiment, High Volatility)
These teams show:
- **Lower sentiment overall**, paired with  
- **strong emotional swings**, often indicating frustration or high expectations.

Examples:
- **New York Yankees (NYY)**
- **Philadelphia Phillies (PHI)**
- **Arizona Diamondbacks (ARI)**

Interpretation:
- These fanbases can be both critical and reactive.
- Volatility may stem from high-pressure markets or historically strong teams with demanding fans.

---

#### D. Stoic or Disengaged Fans (Low Sentiment, Low Volatility)
These teams sit in the “quiet pessimism” region:
- Low emotional positivity  
- Low reactivity or volatility  

Examples:
- **Chicago White Sox (CWS)**
- **Kansas City Royals (KC)**
- **San Francisco Giants (SF)**

Interpretation:
- Fans may feel resigned or disengaged.
- These fanbases generate less emotional fluctuation, even during major events.

---

## Additional Observations

### 2. Playoff Teams Tend to Show Higher Sentiment
Many playoff-qualifying teams fall to the **right side** of the plot — exhibiting above-average sentiment levels.

However:
- Some playoff teams still display **high volatility** (e.g., SEA, LAD), indicating that success doesn’t always stabilize fan emotions.

### 3. Underperforming Teams Cluster in the Lower-Sentiment Regions
Teams like **Washington (WSN)** show low sentiment and moderate volatility — consistent with frustration around performance issues.

### 4. Atlanta Braves Are Unique Outliers
- **Highest average sentiment** combined with **low-to-moderate volatility**.
- Represents a particularly confident and optimistic fanbase with stable emotional tone.

### 5. Volatility Is Not Just a Function of Team Performance
Some strong teams have volatile fanbases (SEA, LAD), while some weaker teams have low volatility (CWS, KC).  
This suggests that:
- Local culture  
- Expectations  
- Historical narratives  
- Media market pressure  

play major roles in shaping fan emotional dynamics.

## Interpretation

Fanbase sentiment cannot be understood solely through win-loss records. Volatility reveals deeper psychological and cultural patterns, including:

- **Optimism vs pessimism**
- **Engagement vs resignation**
- **Expectation pressure vs contentedness**
- **Market-level emotional expression**

These insights describe *how* fans respond, not just *what* they feel.

## Implications for the Chatbot

When explaining fan behavior or interpreting sentiment dynamics:

- Use volatility to differentiate **passionate** vs **disengaged** fanbases.
- Highlight when a team has **more positivity or negativity than performance predicts**.
- Explain outliers like Atlanta (high positivity) or Yankees/Phillies (high negativity + volatility).
- Provide nuanced interpretations:
  - Example: “Mariners fans show high emotional volatility, reflecting both excitement around strong performance and frustration from close losses.”
- Use volatility to frame narrative-driven insights, fan morale analysis, or season storylines.

This enables the chatbot to offer **context-aware, emotionally intelligent** explanations rooted in real data.
