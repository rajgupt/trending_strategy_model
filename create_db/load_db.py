import pandas as pd
import os
from datetime import datetime
from dotenv import load_dotenv
import logging
from sqlalchemy import create_engine, text
from sqlalchemy.exc import IntegrityError

load_dotenv()
DB_URL = os.getenv('DB_URL', 'postgresql://postgres:password@localhost:5432/market_data')
SCHEMA = os.getenv('DB_SCHEMA', 'public')

def get_date_from_filename(filename):
    return filename.split('_')[-1].replace('.csv', '')

def process_detail_file(file_path):
    df = pd.read_csv(file_path)
    report_date = get_date_from_filename(os.path.basename(file_path))
    df['report_date'] = report_date
    return df

def process_trend_file(file_path):
    df = pd.read_csv(file_path)
    report_date = get_date_from_filename(os.path.basename(file_path))
    date_cols = [col for col in df.columns if '-' in col]
    date_cols.sort(reverse=True)
    rename_dict = {date: f'day{i}' for i, date in enumerate(date_cols)}
    df = df.rename(columns=rename_dict)
    df['report_date'] = report_date
    return df

def normalize_column_names(df):
    df.columns = [col.strip().lower().replace(' ', '_') for col in df.columns]
    return df

def create_db(db_url=DB_URL):
    engine = create_engine(db_url, connect_args={"options": f"-csearch_path={SCHEMA}"})
    with engine.connect() as conn:
        conn.execute(text('''
            CREATE TABLE IF NOT EXISTS processed_files (
                filename TEXT PRIMARY KEY,
                processed_at TIMESTAMP
            )
        '''))
        conn.execute(text('''
            CREATE TABLE IF NOT EXISTS stock_prices_processed (
                symbol TEXT,
                last_processed_at TEXT,
                PRIMARY KEY (symbol, last_processed_at)
            )
        '''))
        conn.execute(text('''
            CREATE TABLE IF NOT EXISTS screen5_detail (
                company_name TEXT,
                sector TEXT,
                market_cap DOUBLE PRECISION,
                symbol TEXT,
                isin TEXT,
                weighted_rpi DOUBLE PRECISION,
                "52wk_high_w.rpi" DOUBLE PRECISION,
                latest_price DOUBLE PRECISION,
                "52wk_high" DOUBLE PRECISION,
                ema_9 DOUBLE PRECISION,
                ema_21 DOUBLE PRECISION,
                adx_14 DOUBLE PRECISION,
                rsi_14 DOUBLE PRECISION,
                "2_week_rpi" DOUBLE PRECISION,
                "3_month_rpi" DOUBLE PRECISION,
                "6_month_rpi" DOUBLE PRECISION,
                "2wk_sma_of_6m_rpi" DOUBLE PRECISION,
                report_date TEXT,
                UNIQUE(symbol, report_date)
            )
        '''))
        conn.execute(text('''
            CREATE TABLE IF NOT EXISTS screen5_trend (
                company_name TEXT,
                sector TEXT,
                market_cap DOUBLE PRECISION,
                symbol TEXT,
                isin TEXT,
                "52wk_high" DOUBLE PRECISION,
                trending_days INTEGER,
                day0 DOUBLE PRECISION,
                day1 DOUBLE PRECISION,
                day2 DOUBLE PRECISION,
                day3 DOUBLE PRECISION,
                day4 DOUBLE PRECISION,
                day5 DOUBLE PRECISION,
                day6 DOUBLE PRECISION,
                day7 DOUBLE PRECISION,
                day8 DOUBLE PRECISION,
                day9 DOUBLE PRECISION,
                day10 DOUBLE PRECISION,
                day11 DOUBLE PRECISION,
                weighted_rpi DOUBLE PRECISION,
                adx_14 DOUBLE PRECISION,
                rsi_14 DOUBLE PRECISION,
                report_date TEXT,
                UNIQUE(symbol, report_date)
            )
        '''))
        conn.execute(text('''
            CREATE TABLE IF NOT EXISTS stock_prices (
                report_date TEXT,
                symbol TEXT,
                price DOUBLE PRECISION,
                Open DOUBLE PRECISION,
                Low DOUBLE PRECISION,
                High DOUBLE PRECISION,
                PRIMARY KEY (symbol, report_date)
            )
        '''))
        conn.execute(text('CREATE INDEX IF NOT EXISTS idx_stock_prices_symbol_date ON stock_prices(symbol, report_date)'))
    engine.dispose()

def update_db(folder_path, db_url=DB_URL):
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    engine = create_engine(db_url)
    with engine.begin() as conn:  # Use a transaction block that auto-commits
        def is_processed(filename):
            result = conn.execute(text('SELECT 1 FROM processed_files WHERE filename = :filename'), {'filename': filename})
            return result.fetchone() is not None

        def mark_processed(filename):
            conn.execute(
                text('INSERT INTO processed_files (filename, processed_at) VALUES (:filename, :processed_at)'),
                {'filename': filename, 'processed_at': datetime.now()}
            )

        detail_files = [f for f in os.listdir(folder_path) if 'detail' in f]
        logging.info(f"Found {len(detail_files)} detail files in {folder_path}")
        for file in detail_files:
            if is_processed(file):
                logging.info(f"Detail file already processed: {file}")
                continue
            logging.info(f"Processing detail file: {file}")
            df = process_detail_file(os.path.join(folder_path, file))
            df = normalize_column_names(df)
            try:
                df.to_sql('screen5_detail', engine, if_exists='append', index=False, method='multi')
            except Exception as e:
                logging.error(f"Error inserting detail file {file}: {e}")
            mark_processed(file)
            logging.info(f"Inserted detail file into database: {file}")

        trend_files = [f for f in os.listdir(folder_path) if 'trend' in f]
        logging.info(f"Found {len(trend_files)} trend files in {folder_path}")
        for file in trend_files:
            if is_processed(file):
                logging.info(f"Trend file already processed: {file}")
                continue
            logging.info(f"Processing trend file: {file}")
            df = process_trend_file(os.path.join(folder_path, file))
            df = normalize_column_names(df)
            try:
                df.to_sql('screen5_trend', engine, if_exists='append', index=False, method='multi')
            except Exception as e:
                logging.error(f"Error inserting trend file {file}: {e}")
            mark_processed(file)
            logging.info(f"Inserted trend file into database: {file}")

    engine.dispose()
    logging.info("Database update complete.")

if __name__ == "__main__":
    # create_db(DB_URL)
    update_db('data/screen_detail/5/', DB_URL)
    update_db('data/screen_trend/5/', DB_URL)
    # pass