import requests
import pandas as pd
from datetime import datetime, timezone, timedelta
from dotenv import load_dotenv
import os 

def _generate_datetime_ranges(
        start_date: str, 
        end_date: str, 
    ) -> list[dict[str, datetime]]:
    """ 
    Generates a range of datetime ranges. 
    
    Usage example: 
        _generate_datetime_ranges(start_date="2020-01-01", end_date="2020-01-03")

    Returns: 
            [
                {'start_time': datetime(2020, 1, 1, 0, 0, 0, tzinfo=timezone.utc), 'end_time': datetime(2020, 1, 2, 0, 0, 0, tzinfo=timezone.utc)}, 
                {'start_time': datetime(2020, 1, 2, 0, 0, 0, tzinfo=timezone.utc), 'end_time': datetime(2020, 1, 3, 0, 0, 0, tzinfo=timezone.utc)}
            ]
    
    Args: 
        start_date: provide a str with the format "yyyy-mm-dd"
        end_date: provide a str with the format "yyyy-mm-dd" 

    Returns: 
        A list of dictionaries with datetime objects 
    
    Raises:
        Exception when incorrect input date string format is provided. 
    """

    date_range = []
    if start_date is not None and end_date is not None: 
        raw_start_time = datetime.strptime(start_date, "%Y-%m-%d")
        raw_end_time = datetime.strptime(end_date, "%Y-%m-%d")
        start_time = datetime(
            year=raw_start_time.year, 
            month=raw_start_time.month, 
            day=raw_start_time.day, 
            hour=raw_start_time.hour,
            minute=raw_start_time.minute,
            second=raw_start_time.second,
            tzinfo=timezone.utc
        )
        end_time = datetime(
            year=raw_end_time.year, 
            month=raw_end_time.month, 
            day=raw_end_time.day, 
            hour=raw_end_time.hour,
            minute=raw_end_time.minute,
            second=raw_end_time.second,
            tzinfo=timezone.utc
        )
        date_range = [
            {
                "start_time": (start_time + timedelta(days=i)),
                "end_time": (start_time + timedelta(days=i) + timedelta(days=1)),
            }
        for i in range((end_time - start_time).days)]
    else: 
        raise Exception("Please provide valid dates `YYYY-MM-DD` for start_date and end_date.")
    return date_range  


def extract(api_key_id: str, api_secret_key: str):
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

def load(df_exchange: pd.DataFrame, db_user: str, db_password: str, db_server_name: str, db_database_name: str):
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
    load_dotenv()
    API_KEY_ID = os.environ.get("API_KEY_ID")
    API_SECRET_KEY = os.environ.get("API_SECRET_KEY")
    DB_USERNAME = os.environ.get("DB_USERNAME")
    DB_PASSWORD = os.environ.get("DB_PASSWORD")
    SERVER_NAME = os.environ.get("SERVER_NAME")
    DATABASE_NAME = os.environ.get("DATABASE_NAME")
    df_alpaca_markets = extract(api_key_id=API_KEY_ID, api_secret_key=API_SECRET_KEY)
    df_exchange_codes = extract_exchange_codes("data/exchange_codes.csv")
    df_exchange = transform(df=df_alpaca_markets, df_exchange_codes=df_exchange_codes)
    load(df_exchange, db_user=DB_USERNAME, db_password=DB_PASSWORD, db_server_name=SERVER_NAME, db_database_name=DATABASE_NAME)
