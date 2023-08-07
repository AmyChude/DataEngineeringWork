from dotenv import load_dotenv
import os 
from etl_project.assets.weather import extract_population, extract_weather, transform, load
from etl_project.connectors.postgresql import PostgreSqlClient
from etl_project.connectors.weather_api import WeatherApiClient
from sqlalchemy import Table, MetaData, Column, Integer, String, Float

if __name__ == "__main__": 
    load_dotenv()
    API_KEY = os.environ.get("API_KEY")
    DB_USERNAME = os.environ.get("DB_USERNAME")
    DB_PASSWORD = os.environ.get("DB_PASSWORD")
    SERVER_NAME = os.environ.get("SERVER_NAME")
    DATABASE_NAME = os.environ.get("DATABASE_NAME")
    PORT = os.environ.get("PORT")

    weather_api_client = WeatherApiClient(api_key=API_KEY)
    df_weather = extract_weather(weather_api_client=weather_api_client, city_reference_path="etl_project/data/australian_capital_cities.csv")
    df_population = extract_population(population_reference_path="etl_project/data/australian_city_population.csv")
    df = transform(df_weather=df_weather, df_population=df_population)
    postgresql_client = PostgreSqlClient(
        server_name=SERVER_NAME,
        database_name=DATABASE_NAME,
        username=DB_USERNAME,
        password=DB_PASSWORD,
        port=PORT
    )
    metadata = MetaData()
    table = Table(
        "weather", metadata, 
        Column("datetime", String, primary_key=True),
        Column("id", Integer, primary_key=True),
        Column("name", String),
        Column("temperature", Float),
        Column("population", Integer)
    )
    load(df=df, postgresql_client=postgresql_client, table=table, metadata=metadata)
