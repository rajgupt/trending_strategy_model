import sqlite3
import pandas as pd
import os
from datetime import datetime

def get_date_from_filename(filename):
    # Extract date from filename like screen5_detail_2025-01-03.csv
    return filename.split('_')[-1].replace('.csv', '')

def process_detail_file(file_path):
    df = pd.read_csv(file_path)
    report_date = get_date_from_filename(os.path.basename(file_path))
    df['report_date'] = report_date
    return df

def process_trend_file(file_path):
    df = pd.read_csv(file_path)
    report_date = get_date_from_filename(os.path.basename(file_path))
    
    # Get date columns (excluding the non-date columns)
    date_cols = [col for col in df.columns if '-' in col]
    date_cols.sort(reverse=True)  # Sort to ensure latest date is day0
    
    # Create mapping for renaming
    rename_dict = {date: f'day{i}' for i, date in enumerate(date_cols)}
    
    # Rename columns
    df = df.rename(columns=rename_dict)
    df['report_date'] = report_date
    return df

def normalize_column_names(df):
    # Lowercase, replace spaces with underscores, remove leading/trailing spaces
    df.columns = [col.strip().lower().replace(' ', '_') for col in df.columns]
    return df

def create_db(db_path='market_data.db'):
    conn = sqlite3.connect(db_path)
    # Create tables
    conn.execute('''
        CREATE TABLE IF NOT EXISTS processed_files (
            filename TEXT PRIMARY KEY,
            processed_at TEXT
        )
    ''')
    conn.execute('''
        CREATE TABLE IF NOT EXISTS screen5_detail (
            symbol TEXT,
            -- add other columns as needed
            report_date TEXT
        )
    ''')
    conn.execute('''
        CREATE TABLE IF NOT EXISTS screen5_trend (
            symbol TEXT,
            -- add other columns as needed
            report_date TEXT
        )
    ''')
    # Create unique indexes
    conn.execute('CREATE UNIQUE INDEX IF NOT EXISTS idx_detail_symbol_date ON screen5_detail(symbol, report_date)')
    conn.execute('CREATE UNIQUE INDEX IF NOT EXISTS idx_trend_symbol_date ON screen5_trend(symbol, report_date)')
    conn.close()

def update_db(folder_path, db_path='market_data.db'):
    conn = sqlite3.connect(db_path)

    # Helper to check if file is processed
    def is_processed(filename):
        cur = conn.execute('SELECT 1 FROM processed_files WHERE filename = ?', (filename,))
        return cur.fetchone() is not None

    # Helper to mark file as processed
    def mark_processed(filename):
        conn.execute(
            'INSERT OR IGNORE INTO processed_files (filename, processed_at) VALUES (?, ?)',
            (filename, datetime.now().isoformat())
        )
        conn.commit()

    # Process detail files
    detail_files = [f for f in os.listdir(folder_path) if 'detail' in f]
    for file in detail_files:
        if is_processed(file):
            continue
        df = process_detail_file(os.path.join(folder_path, file))
        df = normalize_column_names(df)
        df.to_sql('screen5_detail', conn, if_exists='append', index=False)
        mark_processed(file)

    # Process trend files
    trend_files = [f for f in os.listdir(folder_path) if 'trend' in f]
    for file in trend_files:
        if is_processed(file):
            continue
        df = process_trend_file(os.path.join(folder_path, file))
        df = normalize_column_names(df)
        df.to_sql('screen5_trend', conn, if_exists='append', index=False)
        mark_processed(file)

    conn.close()

if __name__ == "__main__":
    create_database()