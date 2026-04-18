import pandas as pd

df = pd.read_csv("data/trips_freq.csv")

print("service_type uniques :")
print(df["service_type"].unique().tolist())

print("\nagency_name uniques (top 10) :")
print(df["agency_name"].value_counts().head(10).index.tolist())

print("\nMoyenne fréquence par service_type :")
print(df.groupby("service_type")["frequency_per_week"].mean().round(1).sort_values(ascending=False))