#!/usr/bin/env python3
"""
ETL Pipeline: Garmin Biometric Sleep Data Processing
Author: Portfolio Project
Description: Cleans, transforms, and prepares nested longitudinal Garmin sleep 
             telemetry JSON data into a normalized tabular schema for statistical 
             regression analysis and Tableau BI dashboarding.
"""

import os
import json
import pandas as pd
import numpy as np

def load_raw_json(filepath):
    """Loads the nested raw JSON telemetry file exported from the API."""
    if not os.path.exists(filepath):
        raise FileNotFoundError(f"Target raw telemetry file not found at: {filepath}")
        
    with open(filepath, 'r') as file:
        data = json.load(file)
    return data

def clean_and_transform_sleep_data(raw_data):
    """
    Executes core data cleaning and feature engineering transformations.
    Protects statistical modeling from zero-inflation and scale errors.
    """
    # Convert list of records directly to a pandas DataFrame
    df = pd.DataFrame(raw_data)
    
    print(f"[INFO] Initial ingestion shape: {df.shape[0]} rows, {df.shape[1]} columns.")
    
    # --- STEP 1: Missing Value Mitigation ---
    # Identify rows containing crucial null entries caused by sync timeouts or sensor drop-offs
    crucial_metrics = ['sleep_time_seconds', 'avg_sleep_stress', 'avg_overnight_hrv', 'body_battery_change']
    
    initial_row_count = len(df)
    df = df.dropna(subset=crucial_metrics)
    dropped_rows = initial_row_count - len(df)
    
    if dropped_rows > 0:
        print(f"[CLEANING] Successfully purged {dropped_rows} incomplete row(s) due to null fields.")
    
    # --- STEP 2: Continuous Metric Conversion ---
    # Convert duration metrics from raw seconds to precise continuous decimal hours for proper mapping
    df['sleep_time_hours'] = round(df['sleep_time_seconds'] / 3600, 2)
    
    # --- STEP 3: Zero-Value Data Integrity Verification ---
    # Ensure that zero counts in continuity fields represent true physiological states 
    # and are not artifacts of collection errors or hardware blackouts.
    continuity_checks = ['awake_count', 'restless_moments_count']
    for metric in continuity_checks:
        if metric in df.columns:
            zero_count = (df[metric] == 0).sum()
            print(f"[INTEGRITY] Checked '{metric}': Found {zero_count} natural zero-value nights.")
            
    # --- STEP 4: Feature Reordering & Schema Enforcement ---
    # Explicitly lock and arrange our schema architecture to align with the database dictionary
    ordered_columns =