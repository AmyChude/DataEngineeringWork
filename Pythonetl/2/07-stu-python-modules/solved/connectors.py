import requests
from sqlalchemy import create_engine, Table, Column, Integer, String, MetaData, Float # https://www.tutorialspoint.com/sqlalchemy/sqlalchemy_core_creating_table.htm
from sqlalchemy.engine import URL, Engine
from sqlalchemy.dialects import postgresql
import pandas as pd

def get_alpaca_api_data(stock_ticker: str, start_time: str, end_time: str, api_key_id: str, api_secret_key: str) -> list[dict]:
    base_url = f"https://data.alpaca.markets/v2/stocks/{stock_ticker}/trades"
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
        return response.json().get("trades")
    else: 
        raise Exception(f"Failed to extract data from Open Weather API. Status Code: {response.status_code}. Response: {response.text}")

def get_database_engine(db_user: str, db_password: str, db_server_name: str, db_database_name: str):
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
    return engine 

def write_to_database(engine: Engine, df_exchange: pd.DataFrame):
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
    key_columns = [pk_column.name for pk_column in stock_price_tesla_table.primary_key.columns.values()]
    insert_statement = postgresql.insert(stock_price_tesla_table).values(df_exchange.to_dict(orient='records'))
    upsert_statement = insert_statement.on_conflict_do_update(
        index_elements=key_columns,
        set_={c.key: c for c in insert_statement.excluded if c.key not in key_columns})
    engine.execute(upsert_statement)
