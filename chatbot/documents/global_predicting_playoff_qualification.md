We next modeled the probability that a team makes the playoffs using a logistic regression classifier with team performance metrics as predictors.

- **Model type:** Logistic regression  
- **Target:** Playoff qualification (yes/no)  
- **Performance:**  
  - ROC AUC ≈ 0.97  
  - Accuracy ≈ 92% on training data  

These results indicate that playoff teams can be separated from non-playoff teams with high confidence based on performance statistics alone.

Key insights:

- As in the win-percentage model, **runs allowed is the single most important feature**, reinforcing the finding that preventing runs is central to team success.
- Other strong predictors include:
  - **Run differential**
  - **ERA**
  - **Fielding percentage**
  - **WHIP**
- Offensive metrics such as **OBP**, **OPS**, and **total runs scored** have **smaller but still significant** effects on playoff odds.

An additional non-intuitive result is that **payroll shows a slightly negative effect** on playoff likelihood. This suggests that in the recent data, **high-budget teams may be underperforming relative to their financial resources**, possibly due to inefficiency in how resources are deployed.

**Core takeaway for the chatbot:**  
When discussing playoff chances or “playoff-caliber” teams, the model should highlight **run suppression, run differential, and defensive stability** more than sheer spending or offensive firepower.

