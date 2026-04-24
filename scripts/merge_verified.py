"""
merge_verified.py
-----------------
Merges all *_list1_qualified.xlsx files in output/,
keeps only VERIFIED emails, deduplicates, saves one clean Excel.

Usage: python scripts/merge_verified.py
"""
import pandas as pd
from pathlib import Path

OUTPUT_DIR = Path(__file__).parents[1] / "output"

files = list(OUTPUT_DIR.glob("*_list1_qualified.xlsx"))
if not files:
    print("No qualified files found in output/")
    exit()

df = pd.concat([pd.read_excel(f) for f in files], ignore_index=True)
print(f"Total rows across {len(files)} files: {len(df):,}")

df = df[df["Email Status"] == "VERIFIED"]
print(f"After VERIFIED filter: {len(df):,}")

df = df.drop_duplicates(subset=["Best Email"], keep="first")
print(f"After dedup: {len(df):,}")

out = OUTPUT_DIR / "email_blast_verified.xlsx"
df.to_excel(out, index=False)
print(f"Saved: {out}")
