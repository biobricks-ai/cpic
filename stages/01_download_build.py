#!/usr/bin/env python3
"""
Download and build CPIC pharmacogenomics data.
Clinical Pharmacogenetics Implementation Consortium (CPIC) provides
gene-drug recommendations for clinical pharmacogenomics.
Source: https://cpicpgx.org/api-and-database/
"""

import os
import json
from pathlib import Path
import pandas as pd
import requests

BASE_URL = "https://api.cpicpgx.org/v1"

def fetch_endpoint(endpoint: str) -> list:
    """Fetch all data from a CPIC API endpoint."""
    print(f"Fetching {endpoint}...")
    response = requests.get(f"{BASE_URL}/{endpoint}")
    response.raise_for_status()
    data = response.json()
    print(f"  - Retrieved {len(data)} records")
    return data

def flatten_dict_column(df: pd.DataFrame, col: str) -> pd.DataFrame:
    """Flatten a dictionary column into JSON string for parquet compatibility."""
    if col in df.columns:
        df[col] = df[col].apply(lambda x: json.dumps(x) if isinstance(x, dict) else x)
    return df

def flatten_list_column(df: pd.DataFrame, col: str) -> pd.DataFrame:
    """Flatten a list column into JSON string for parquet compatibility."""
    if col in df.columns:
        df[col] = df[col].apply(lambda x: json.dumps(x) if isinstance(x, list) else x)
    return df

def main():
    brick_path = Path("brick")
    brick_path.mkdir(exist_ok=True)

    print("=" * 60)
    print("Downloading CPIC pharmacogenomics data...")
    print("=" * 60)

    # Fetch all endpoints
    endpoints = {
        'gene': 'gene',
        'allele': 'allele',
        'drug': 'drug',
        'guideline': 'guideline',
        'recommendation': 'recommendation',
        'diplotype': 'diplotype',
        'pair': 'pair',  # gene-drug pairs
    }

    dataframes = {}

    for name, endpoint in endpoints.items():
        try:
            data = fetch_endpoint(endpoint)
            if data:
                df = pd.DataFrame(data)

                # Standardize column names
                df.columns = [str(c).strip().lower().replace(' ', '_').replace('-', '_') for c in df.columns]

                # Flatten any dict/list columns for parquet compatibility
                for col in df.columns:
                    if df[col].dtype == 'object':
                        sample = df[col].dropna().head(1).values
                        if len(sample) > 0:
                            if isinstance(sample[0], dict):
                                df = flatten_dict_column(df, col)
                            elif isinstance(sample[0], list):
                                df = flatten_list_column(df, col)

                dataframes[name] = df

                # Save to parquet
                output_file = brick_path / f"cpic_{name}.parquet"
                df.to_parquet(output_file, index=False)
                print(f"  - Saved {len(df)} records to {output_file}")
        except Exception as e:
            print(f"  - Error fetching {name}: {e}")

    # Create allele function summary (key for CYP allele lookups)
    if 'allele' in dataframes:
        print("\n" + "=" * 60)
        print("Creating allele function summary...")
        print("=" * 60)

        allele_df = dataframes['allele']
        # Key columns for allele function lookups
        key_cols = ['id', 'genesymbol', 'name', 'functionalstatus',
                    'clinicalfunctionalstatus', 'activityvalue', 'strength']
        available_cols = [c for c in key_cols if c in allele_df.columns]

        allele_summary = allele_df[available_cols].copy()
        summary_file = brick_path / "cpic_allele_functions.parquet"
        allele_summary.to_parquet(summary_file, index=False)
        print(f"  - Saved allele function summary: {len(allele_summary)} records")

    # Create recommendation summary (key for clinical use)
    if 'recommendation' in dataframes:
        print("\n" + "=" * 60)
        print("Creating recommendation summary...")
        print("=" * 60)

        rec_df = dataframes['recommendation']
        summary_file = brick_path / "cpic_recommendations.parquet"
        # Already saved above, just note it
        print(f"  - Recommendation data available with {len(rec_df)} gene-drug recommendations")

    # Print summary
    print("\n" + "=" * 60)
    print("Output files:")
    print("=" * 60)
    total_records = 0
    for f in sorted(brick_path.glob("*.parquet")):
        df = pd.read_parquet(f)
        total_records += len(df)
        print(f"  - {f.name}: {len(df)} rows, {len(df.columns)} columns")

    print(f"\nTotal: {total_records} records across {len(list(brick_path.glob('*.parquet')))} files")

if __name__ == "__main__":
    main()
