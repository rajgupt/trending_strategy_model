# Load the tables screen5_detail and screen5_trend from the database
import requests
import os
import sqlite3
import pandas as pd
from datetime import datetime
from dotenv import load_dotenv
import logging
from sqlalchemy import create_engine, text
from sqlalchemy.exc import IntegrityError

# configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
# load environment variables
load_dotenv()
DB_URL = os.getenv('DB_URL', 'postgresql://postgres:password@localhost:5432/market_data')
SCHEMA = os.getenv('DB_SCHEMA', 'public')

# read the screen5_trend tables from the database
def read_screen5_trend(db_path=DB_URL):
    engine = create_engine(DB_URL, connect_args={"options": f"-csearch_path={SCHEMA}"})
    query = "SELECT * FROM screen5_trend"
    with engine.connect() as conn:
        df = pd.read_sql_query(query, conn)
    return df

# for each symbol and report date get the stock price from the yfinance API
def get_stock_prices(symbol, report_date):
    import yfinance as yf
    try:
        stock = yf.Ticker(symbol + '.NS')
        hist = stock.history(start=report_date)
        hist.reset_index(inplace=True)
        hist['symbol'] = symbol
        return hist[['Date', 'symbol', 'Close', 'Open', 'Low', 'High']].rename(columns={'Date': 'report_date', 'Close': 'price'})
    except Exception as e:
        logging.error(f"Error fetching data for {symbol}: {e}")
        return None

# Get unique symbols and report dates from the DataFrame
def get_unique_symbols_and_dates(df):
    symbols = df['symbol'].unique()
    start_date = df['report_date'].min()
    return symbols, start_date


def main(df):
    symbols, start_date = get_unique_symbols_and_dates(df)
    all_prices = {}

    print(f"Fetching stock prices for report date since {start_date}")
    stock_df_list = []

    # Use SQLAlchemy engine for Postgres
    engine = create_engine(DB_URL, connect_args={"options": f"-csearch_path={SCHEMA}"})

    for s in symbols:
        logging.info(f"Fetching stock prices for symbol: {s}")

        # Check if stock prices for this symbol have already been processed
        with engine.connect() as conn:
            result = conn.execute(
                text('SELECT last_processed_at FROM stock_prices_processed WHERE symbol = :symbol'),
                {'symbol': s}
            ).fetchone()

        symbol_start_date = start_date
        if result and result[0]:
            last_processed_date = result[0]
            if str(last_processed_date) >= str(start_date):
                # Set start_date to the next day after last_processed_date
                symbol_start_date = (pd.to_datetime(last_processed_date) + pd.Timedelta(days=1)).strftime('%Y-%m-%d')

        logging.info(f"Fetching stock prices for symbol: {s} and start date: {symbol_start_date}")
        stock_df = get_stock_prices(s, symbol_start_date)
        if stock_df is not None and not stock_df.empty:
            stock_df_list.append(stock_df)
        else:
            logging.warning(f"No data found for symbol {s} on or after {symbol_start_date}")

    # Convert to DataFrame for better visualization
    if stock_df_list:
        prices_df = pd.concat(stock_df_list, ignore_index=True)
    else:
        logging.error("No stock prices data available.")
        return

    # saving temporary CSV file in case of db failure. 
    prices_df.to_csv(f'data/stock_prices_{}.csv', index=False)

    # Save to Postgres database
    with engine.begin() as conn:
        prices_df.to_sql('stock_prices', conn, schema=SCHEMA, if_exists='replace', index=False, method='multi')

        # store the last processed date for each symbol
        for symbol in symbols:
            last_processed_date = prices_df[prices_df['symbol'] == symbol]['report_date'].max()
            if pd.isna(last_processed_date):
                continue
            conn.execute(
                text('''
                    INSERT INTO stock_prices_processed (symbol, last_processed_at)
                    VALUES (:symbol, :last_processed_at)
                    ON CONFLICT (symbol) DO UPDATE SET last_processed_at = EXCLUDED.last_processed_at
                '''),
                {'symbol': symbol, 'last_processed_at': str(last_processed_date)}
            )

    print(f"Stock prices saved to Postgres in table '{SCHEMA}.stock_prices'.")
    logging.info(f"Stock prices saved to Postgres in table '{SCHEMA}.stock_prices'.")
    print(f"Processed {len(symbols)} symbols with data since {start_date}.")
    logging.info(f"Processed {len(symbols)} symbols with data since {start_date}.")


if __name__ == "__main__":
    df = read_screen5_trend()
    main(df)
