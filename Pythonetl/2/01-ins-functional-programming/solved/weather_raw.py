import requests
import pandas as pd 
from secrets_config import api_key # https://home.openweathermap.org/ 

# read list of cities
df_cities = pd.read_csv("data/australian_capital_cities.csv")
df_cities.head()

# request data for each city (json) and push to a list 
weather_data = []
for city_name in df_cities["city_name"]:
    params = {
        "q": city_name,
        "units": "metric",
        "appid": api_key
    }
    response = requests.get(f"http://api.openweathermap.org/data/2.5/weather", params=params)
    if response.status_code == 200: 
        weather_data.append(response.json())
    else: 
        raise Exception("Extracting weather api data failed. Please check if API limits have been reached.")
    
# convert list into dataframe 
df_weather_cities = pd.json_normalize(weather_data)

# set city names to lowercase 
df_weather_cities["city_name"] = df_weather_cities["name"].str.lower()

df_population = pd.read_csv("data/australian_city_population.csv")

df_merged = pd.merge(left=df_weather_cities, right=df_population, on=["city_name"])

df_selected = df_merged[["dt", "id", "name", "main.temp", "population"]] 

df_selected["unique_id"] = df_selected["dt"].astype(str) + df_selected["id"].astype(str)

# convert unix timestamp column to datetime 
df_selected["dt"] = pd.to_datetime(df_selected["dt"], unit="s")

# rename colum names to more meaningful names
df_selected = df_selected.rename(columns={
    "dt": "datetime",
    "main.temp": "temperature"
})
df_selected = df_selected.set_index(["unique_id"])

# load (overwrite) data to a csv file 
df_selected.to_parquet("data/weather.parquet")

import datetime as dt 
current_timestamp = dt.datetime.now().isoformat().replace(":","-")
df_selected.to_parquet(f"data/weather_{current_timestamp}.parquet")

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


# using pandas 
df_selected.to_sql("weather_ins", engine, if_exists="append")

# using pandas 
df_selected.to_sql("weather_ins", engine, if_exists="replace")

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

insert_statement = postgresql.insert(weather_table).values(df_selected.to_dict(orient='records'))
upsert_statement = insert_statement.on_conflict_do_update(
    index_elements=['id', 'datetime'],
    set_={c.key: c for c in insert_statement.excluded if c.key not in ['id', 'datetime']})
engine.execute(upsert_statement)