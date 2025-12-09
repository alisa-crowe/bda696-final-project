We fit a multiple linear regression model to predict team win percentage using standardized team-level performance metrics from the 2025 MLB regular season.

- **Model type:** Multiple linear regression  
- **Target:** Win percentage  
- **Performance:**  
  - R² ≈ 0.90  
  - RMSE ≈ 0.014  

These metrics indicate very strong predictive power without requiring extra feature engineering or parameter tuning.

A key finding was a **negative coefficient on strikeouts per nine innings (K/9)**. Although K/9 is typically seen as a positive indicator of pitching quality, its negative effect in this multivariate context likely reflects:

- Overlap with other pitching metrics such as **ERA** and **WHIP**, which may be capturing most of the true signal about pitching quality.
- The fact that **high K/9 can coexist with drawbacks** like higher walk rates, elevated pitch counts, and heavier reliance on the bullpen.
- K/9 may be a better descriptor of **individual pitcher dominance** than of **overall staff efficiency**.

Overall, the regression results show that **defensive and pitching metrics dominate offensive metrics** in explaining team success. In particular, **runs allowed** emerges as the strongest predictor of win percentage. This aligns with prior sabermetric work (e.g., Tango et al., 2007) but may be counterintuitive to fans who focus on offensive fireworks.

**Core takeaway for the chatbot:**  
When advising users on building successful teams, the model should emphasize **run prevention (runs allowed, ERA, WHIP, defensive quality)** more than raw offensive power.
