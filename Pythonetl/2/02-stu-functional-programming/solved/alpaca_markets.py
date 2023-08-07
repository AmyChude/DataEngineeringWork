import requests
# get alpaca api keys using this guide: https://alpaca.markets/docs/market-data/getting-started/#creating-an-alpaca-account-and-finding-your-api-keys
from secrets_config import api_key_id, api_secret_key 
import pandas as pd
from datetime import datetime, timezone

def extract():
    stock_ticker = "tsla" # tlsa maps to tesla
    base_url = f"https://data.alpaca.markets/v2/stocks/{stock_ticker}/trades"
    start_time = datetime(2020, 1, 1, tzinfo=timezone.utc).isoformat()
    end_time = datetime(2020, 1, 2, tzinfo=timezone.utc).isoformat()

    response_data = []

    params = {
        "start": start_time,
        "end": end_time
    }

    headers = {
        "APCA-API-KEY-ID": api_key_id,
        "APCA-API-SECRET-KEY": api_secret_key
    }
    response = requests.get(base_url, params=params, headers=headers)
    if response.json().get("trades") is not None: 
        response_data.extend(response.json().get("trades"))
    df_quotes = pd.json_normalize(data=response_data, meta=["symbol"])
    return df_quotes

def extract_exchange_codes(csv_path):
    return pd.read_csv(csv_path)

def transform(df: pd.DataFrame, df_exchange_codes: pd.DataFrame) -> pd.DataFrame:
    # rename columns to more meaningful names
    df_quotes_renamed = df.rename(columns={
        "i": "id",
        "t": "timestamp",
        "x": "exchange",
        "p": "price",
        "s": "size",
    })
    # keep only 'id', 'timestamp', 'exchange', 'price', 'size' columns 
    df_quotes_selected = df_quotes_renamed[['id', 'timestamp', 'exchange', 'price', 'size']]
    df_exchange = pd.merge(left=df_quotes_selected, right=df_exchange_codes, left_on="exchange", right_on="exchange_code").drop(columns=["exchange_code", "exchange"]).rename(columns={"exchange_name": "exchange"})
    return df_exchange

def load(df_exchange: pd.DataFrame):
    from sqlalchemy import create_engine, Table, Column, Integer, String, MetaData, Float # https://www.tutorialspoint.com/sqlalchemy/sqlalchemy_core_creating_table.htm
    from sqlalchemy.engine import URL
    from sqlalchemy.dialects import postgresql
    from secrets_config import db_user, db_password, db_server_name, db_database_name
    from sqlalchemy.schema import CreateTable 

    # create connection to database 
    connection_url = URL.create(
        drivername = "postgresql+pg8000", 
        username = db_user,
        password = db_password,
        host = db_server_name, 
        port = 5432,
        database = db_database_name, 
    )

    engine = create_engine(connection_url)

    meta = MetaData()
    stock_price_tesla_table = Table(
        "stock_price_tesla", meta, 
        Column("id", Integer, primary_key=True),
        Column("exchange", String, primary_key=True),
        Column("timestamp", String, primary_key=True),
        Column("price", Float),
        Column("size", Integer)
    )
    meta.create_all(engine) # creates table if it does not exist 

    insert_statement = postgresql.insert(stock_price_tesla_table).values(df_exchange.to_dict(orient='records'))
    upsert_statement = insert_statement.on_conflict_do_update(
        index_elements=['id', 'exchange', 'timestamp'],
        set_={c.key: c for c in insert_statement.excluded if c.key not in ['id', 'exchange', 'timestamp']})
    engine.execute(upsert_statement)

if __name__ == "__main__":
    df_alpaca_markets = extract()
    df_exchange_codes = extract_exchange_codes("data/exchange_codes.csv")
    df_exchange = transform(df=df_alpaca_markets, df_exchange_codes=df_exchange_codes)
    load(df_exchange)
