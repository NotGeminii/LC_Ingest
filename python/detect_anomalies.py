# File: detect_anomalies.py

import pyodbc
import pandas as pd
from sklearn.ensemble import IsolationForest

# 1. Connect to your staging/fact database
cn = pyodbc.connect(
    "DRIVER={ODBC Driver 17 for SQL Server};"
    "SERVER=localhost;DATABASE=loan_ingest;Trusted_Connection=yes;"
)

# 2. Pull today’s fact rows
df = pd.read_sql(
    "SELECT loan_key, loan_amnt, int_rate, annual_inc "
    "FROM fact_loans "
    "WHERE issue_d = CAST(GETDATE() AS DATE)",
    cn
)

# 3. If there’s data, fit an IsolationForest and flag anomalies
if not df.empty:
    model = IsolationForest(contamination=0.01, random_state=42)
    df["anomaly"] = model.fit_predict(
        df[["loan_amnt","int_rate","annual_inc"]]
    ) == -1

    # 4. Write any anomalies back to SQL
    df[df.anomaly].to_sql(
        "fact_loans_anomaly",
        cn,
        if_exists="append",
        index=False
    )
