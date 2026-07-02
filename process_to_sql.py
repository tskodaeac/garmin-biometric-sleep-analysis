#!/usr/bin/env python3
"""
Database Loading Pipeline: Tabular CSV to Relational SQL
Author: Portfolio Project
Description: Ingests the normalized Garmin biometric sleep dataset, establishes 
             a structured local SQL database relational schema (SQLite), and executes 
             transaction-safe loading with primary key validation to prevent record duplication.
"""

import os
import sqlite3
import pandas as pd

def load_clean_csv(filepath):
    """Ingests the fully transformed tabular CSV file into a pandas DataFrame."""
    if not os.path.exists(filepath):
        raise FileNotFoundError(f"Target cleaned CSV file not found at: {filepath}")
    
    df = pd.read_csv(filepath)
    print(f"[INFO] Successfully loaded cleaned dataset with {len(df)} records for SQL migration.")
    return df

def initialize_database(db_name="garmin_health_analytics.db"):
    """
    Establishes a connection to the SQLite database and initializes the relational schema.
    Enforces rigid data types and primary key constraints.
    """
    conn = sqlite3.connect(db_name)
    cursor = conn.cursor()
    
    # Enable foreign key support if schema expands in production
    cursor.execute("PRAGMA foreign_keys = ON;")
    
    # Create the structured primary analytics table
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS sleep_telemetry (
        calendar_date TEXT PRIMARY KEY,
        sleep_time_seconds INTEGER NOT NULL,
        sleep_time_hours REAL NOT NULL,
        deep_sleep_seconds INTEGER,
        light_sleep_seconds INTEGER,
        rem_sleep_seconds INTEGER,
        awake_sleep_seconds INTEGER,
        awake_count INTEGER,
        restless_moments_count INTEGER,
        avg_sleep_stress INTEGER NOT NULL,
        avg_overnight_hrv INTEGER NOT NULL,
        avg_heart_rate INTEGER,
        average_spo2 REAL,
        lowest_spo2 REAL,
        respiration_value REAL,
        body_battery_change INTEGER NOT NULL,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """)
    
    conn.commit()
    print(f"[DATABASE] Initialized SQLite instance: '{db_name}' with verified tables.")
    return conn

def populate_table(conn, dataframe, table_name="sleep_telemetry"):
    """
    Executes transaction-safe data loading into the destination database table.
    Implements an 'INSERT OR REPLACE' rule to protect database updates from primary key duplication.
    """
    cursor = conn.cursor()
    
    # Convert DataFrame records to an iterable list of tuples matching the database columns
    # We select columns explicitly to ensure strict alignment with the SQL table definition
    db_columns = [
        'calendar_date', 'sleep_time_seconds', 'sleep_time_hours', 
        'deep_sleep_seconds', 'light_sleep_seconds', 'rem_sleep_seconds', 
        'awake_sleep_seconds', 'awake_count', 'restless_moments_count', 
        'avg_sleep_stress', 'avg_overnight_hrv', 'avg_heart_rate', 
        'average_spo2', 'lowest_spo2', 'respiration_value', 'body_battery_change'
    ]
    
    # Filter the DataFrame to ensure only the columns defined in the table are exported
    export_df = dataframe[[col for col in db_columns if col in dataframe.columns]]
    records = export_df.to_records(index=False)
    records_list = [tuple(x) for x in records]
    
    # Construct parameterized SQL query to protect against injection issues
    placeholders = ", ".join(["?"] * len(export_df.columns))
    columns_joined = ", ".join(export_df.columns)
    sql_query = f"INSERT OR REPLACE INTO {table_name} ({columns_joined}) VALUES ({placeholders})"
    
    try:
        # Execute batch transaction loading
        cursor.executemany(sql_query, records_list)
        conn.commit()
        print(f"[LOADING] SQL transactional write successful. Migrated {len(records_list)} rows into table '{table_name}'.")
    except sqlite3.Error as e:
        conn.rollback()
        print(f"[CRITICAL ERROR] Database transaction failed. Rolling back changes. Context: {e}")
        raise e

def run_analytical_verification_query(conn):
    """Executes a diagnostic SQL aggregation query to verify database health and record availability."""
    print("\n" + "="*50)
    print("RUNNING DATABASE DIAGNOSTICS & AGGREGATIONS")
    print("="*50)
    
    query = """
    SELECT 
        COUNT(*) as total_logged_nights,
        ROUND(AVG(sleep_time_hours), 2) as average_sleep_hours,
        ROUND(AVG(avg_sleep_stress), 1) as structural_average_stress,
        ROUND(AVG(avg_overnight_hrv), 1) as population_average_hrv,
        ROUND(AVG(body_battery_change), 1) as average_net_recovery
    FROM sleep_telemetry;
    """
    
    try:
        metrics_df = pd.read_sql_query(query, conn)
        print(metrics_df.to_string(index=False))
    except Exception as e:
        print(f"[ERROR] Diagnostic query execution failed. Context: {e}")
    print("="*50 + "\n")

if __name__ == "__main__":
    # Define system path configurations
    CLEANED_CSV_PATH = "garmin_sleep_master - garmin_sleep_master_clean.csv"
    SQLITE_DB_NAME = "garmin_health_analytics.db"
    
    print("="*60)
    print("STARTING DATABASE STORAGE LOADING PIPELINE: CSV TO RELATIONAL SQL")
    print("="*60)
    
    try:
        # 1. Ingest clean data
        cleaned_dataframe = load_clean_csv(CLEANED_CSV_PATH)
        
        # 2. Open secure database connection and declare schema rules
        db_connection = initialize_database(SQLITE_DB_NAME)
        
        # 3. Populate database table with structural safety handling
        populate_table(db_connection, cleaned_dataframe)
        
        # 4. Run automated test query to verify production health
        run_analytical_verification_query(db_connection)
        
        # 5. Safely drop connection instance
        db_connection.close()
        print("[SUCCESS] Database connection securely terminated. Pipeline run completed.")
        
    except Exception as e:
        print(f"[CRITICAL FAILURE] Pipeline execution halted. Technical Context: {e}")
    
    print("="*60)