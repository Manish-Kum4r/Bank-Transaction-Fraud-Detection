Bank Transaction Fraud Detection
Python Analysis using NumPy + Pandas Only

import pandas as pd
import numpy as np

 ─────────────────────────────────────────────
 1. LOAD DATA
 ─────────────────────────────────────────────
df = pd.read_csv("bank_transactions.csv", parse_dates=["timestamp"])
print("=" * 60)
print(f"Dataset loaded: {len(df):,} transactions")
print(f"Columns: {list(df.columns)}")
print(df.dtypes)
print()

 ─────────────────────────────────────────────
 2. FEATURE ENGINEERING
 ─────────────────────────────────────────────
df["hour"]   = df["timestamp"].dt.hour
df["minute"] = df["timestamp"].dt.minute
df["date"]   = df["timestamp"].dt.date

 ── Rule 1: High-amount flag (> ₹1,00,000) ──
df["high_amount_flag"] = np.where(df["amount"] > 100_000, 1, 0)

 ── Rule 2: Odd-hour flag (midnight to 4 AM) ──
df["odd_hour_flag"] = np.where(df["hour"].between(0, 3), 1, 0)

 ── Rule 3: Rapid transaction flag ──
    More than 5 transactions by the same customer within any 10-minute window
df_sorted = df.sort_values(["customer_id", "timestamp"]).reset_index(drop=True)
df_sorted["ts_epoch"] = df_sorted["timestamp"].astype(np.int64) // 10**9  # seconds

rapid_flags = np.zeros(len(df_sorted), dtype=int)

for cust, grp in df_sorted.groupby("customer_id"):
    idxs = grp.index.values
    times = grp["ts_epoch"].values
    for i, (idx, t) in enumerate(zip(idxs, times)):
        window_mask = (times >= t) & (times <= t + 600)   # 10-minute window
        if window_mask.sum() > 5:
            rapid_flags[idx] = 1

df_sorted["rapid_txn_flag"] = rapid_flags

 ─────────────────────────────────────────────
 3. FRAUD SCORE
 ─────────────────────────────────────────────
 Score = weighted sum of flags (max = 5)
df_sorted["fraud_score"] = (
    df_sorted["high_amount_flag"] * 2 +
    df_sorted["odd_hour_flag"]    * 1 +
    df_sorted["rapid_txn_flag"]   * 2
)

 Potential fraud if score >= 2
df_sorted["potential_fraud"] = np.where(df_sorted["fraud_score"] >= 2, 1, 0)

 ─────────────────────────────────────────────
 4. SUMMARY OUTPUTS
 ─────────────────────────────────────────────
print("=" * 60)
print("FRAUD vs NON-FRAUD (original labels)")
print("-" * 40)
counts = df_sorted["fraud_flag"].value_counts()
print(f"  Non-Fraud : {counts.get(0, 0):>6,}  ({counts.get(0,0)/len(df_sorted)*100:.1f}%)")
print(f"  Fraud     : {counts.get(1, 0):>6,}  ({counts.get(1,0)/len(df_sorted)*100:.1f}%)")
print()

print("RULE-BASED FLAGS SUMMARY")
print("-" * 40)
print(f"  High Amount Flag  : {df_sorted['high_amount_flag'].sum():>6,}")
print(f"  Odd Hour Flag     : {df_sorted['odd_hour_flag'].sum():>6,}")
print(f"  Rapid Txn Flag    : {df_sorted['rapid_txn_flag'].sum():>6,}")
print(f"  Potential Fraud   : {df_sorted['potential_fraud'].sum():>6,}")
print()

print("FRAUD SCORE DISTRIBUTION")
print("-" * 40)
score_dist = df_sorted["fraud_score"].value_counts().sort_index()
for score, cnt in score_dist.items():
    bar = "█" * (cnt // 100)
    print(f"  Score {score}: {cnt:>5,}  {bar}")
print()

 ── Top 10 Risky Customers ──
print("TOP 10 RISKY CUSTOMERS")
print("-" * 60)
risky = (
    df_sorted.groupby("customer_id")
    .agg(
        total_transactions=("transaction_id", "count"),
        total_amount=("amount", "sum"),
        avg_fraud_score=("fraud_score", "mean"),
        flagged_txns=("potential_fraud", "sum"),
    )
    .sort_values("flagged_txns", ascending=False)
    .head(10)
    .reset_index()
)
risky["total_amount"] = risky["total_amount"].apply(lambda x: f"₹{x:,.0f}")
risky["avg_fraud_score"] = risky["avg_fraud_score"].round(2)
print(risky.to_string(index=False))
print()

 ─────────────────────────────────────────────
 5. EXPORT ENRICHED CSV FOR POWER BI
 ─────────────────────────────────────────────
export_cols = [
    "transaction_id", "customer_id", "amount", "timestamp",
    "location", "fraud_flag", "hour", "date",
    "high_amount_flag", "odd_hour_flag", "rapid_txn_flag",
    "fraud_score", "potential_fraud"
]
df_sorted[export_cols].to_csv("bank_transactions_enriched.csv", index=False)
print("✅  Exported: bank_transactions_enriched.csv  (use this in Power BI)")
print("=" * 60)
