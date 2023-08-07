import requests 
from sqlalchemy import create_engine, Table, Column, Integer, String, MetaData, Float # https://www.tutorialspoint.com/sqlalchemy/sqlalchemy_core_creating_table.htm
from sqlalchemy.engine import URL, Engine
from sqlalchemy.dialects import postgresql
import pandas as pd

def get_weather_api(city_name: str, api_key: str) -> dict:
    params = {
        "q": city_name,
        "units": "metric",
        "appid": api_key
    }
    response = requests.get(f"http://api.openweathermap.org/data/2.5/weather", params=params)
    if response.status_code == 200: 
        return response.json()
    else: 
        raise Exception(f"Failed to extract data from Open Weather API. Status Code: {response.status_code}. Response: {response.text}")

def get_engine(db_user: str, db_password: str, db_server_name: str, db_database_name: str) -> Engine: 
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

def write_to_database(df: pd.DataFrame, engine: Engine):
    meta = MetaData()
    weather_table = Table(
        "weather", meta, 
        Column("datetime", String, primary_key=True),
        Column("id", Integer, primary_key=True),
        Column("name", String),
        Column("temperature", Float),
        Column("population", Integer)
    )
    meta.create_all(engine) # creates table if it does not exist 
    key_columns = [pk_column.name for pk_column in weather_table.primary_key.columns.values()]
    insert_statement = postgresql.insert(weather_table).values(df.to_dict(orient='records'))
    upsert_statement = insert_statement.on_conflict_do_update(
        index_elements=key_columns,
        set_={c.key: c for c in insert_statement.excluded if c.key not in key_columns})
    engine.execute(upsert_statement)
