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
import argparse

# add the argparse setup
parser = argparse.ArgumentParser(description='Get stock prices for symbols in screen5_trend')
# argument for start_date of stock prices to be fetched
parser.add_argument('--start_date', type=str, required=True, help='Start date in YYYY-MM-DD format')
# add argument for end_date of stock prices to be fetched
parser.add_argument('--end_date', type=str, default=None, help='End date in YYYY-MM-DD format (optional)')
args = parser.parse_args()
end_date = args.end_date
start_date = args.start_date


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
def get_stock_prices(symbol, start_date, end_date=None):
    import yfinance as yf
    try:
        stock = yf.Ticker(symbol + '.NS')
        if end_date:
            hist = stock.history(start=start_date, end=end_date)
        else:
            hist = stock.history(start=start_date)
        hist.reset_index(inplace=True)
        hist['symbol'] = symbol
        return hist[['Date', 'symbol', 'Close', 'Open', 'Low', 'High', 'Volume']].rename(columns={'Date': 'report_date', 'Close': 'price'})
    except Exception as e:
        logging.error(f"Error fetching data for {symbol}: {e}")
        return None

# Get unique symbols and report dates from the DataFrame
def get_unique_symbols_and_dates(df):
    min_dates = df.groupby('symbol')['report_date'].min().reset_index()
    min_dates = min_dates.rename(columns={'report_date': 'start_date'})
    return min_dates


def main(df):
    min_dates = get_unique_symbols_and_dates(df)
    all_prices = {}

    stock_df_list = []

    # Use SQLAlchemy engine for Postgres
    engine = create_engine(DB_URL, connect_args={"options": f"-csearch_path={SCHEMA}"})

    for idx, row in min_dates.iterrows():
        s = row['symbol']
        # start_date = row['start_date']
        logging.info(f"Fetching stock prices for symbol: {s}")

        symbol_start_date = start_date

        logging.info(f"Fetching stock prices for symbol: {s} and start date: {symbol_start_date}")
        if end_date is not None:
            stock_df = get_stock_prices(s, symbol_start_date, end_date)
        else:
            # If end_date is not provided, fetch data from start_date to today
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

    # # saving temporary CSV file in case of db failure. 
    # # today's date is appended to the filename
    today = datetime.now().strftime('%Y-%m-%d-%H-%M-%S')
    prices_df.to_csv(f'data/stock_prices_{today}.csv', index=False)
    # prices_df = pd.read_csv(f'data/stock_prices_{today}.csv', parse_dates=['report_date'])
    # Save to Postgres database
    with engine.begin() as conn:
        prices_df.to_sql('stock_prices', conn, schema=SCHEMA, if_exists='replace', index=False, method='multi')

        # store the last processed date for each symbol
        for symbol in min_dates['symbol'].unique():
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
    

if __name__ == "__main__":
    df = read_screen5_trend()
    main(df)
