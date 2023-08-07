from dotenv import load_dotenv
import os 
from etl_project.assets.weather import extract_population, extract_weather, transform, load
from etl_project.connectors.postgresql import PostgreSqlClient
from etl_project.connectors.weather_api import WeatherApiClient
from sqlalchemy import Table, MetaData, Column, Integer, String, Float
from etl_project.assets.pipeline_logging import PipelineLogging

if __name__ == "__main__": 
    pipeline_logging = PipelineLogging(pipeline_name="weather", log_folder_path="etl_project/logs")
    load_dotenv()
    pipeline_logging.logger.info("Starting pipeline run")
    pipeline_logging.logger.info("Getting pipeline environment variables")
    API_KEY = os.environ.get("API_KEY")
    DB_USERNAME = os.environ.get("DB_USERNAME")
    DB_PASSWORD = os.environ.get("DB_PASSWORD")
    SERVER_NAME = os.environ.get("SERVER_NAME")
    DATABASE_NAME = os.environ.get("DATABASE_NAME")
    PORT = os.environ.get("PORT")
    pipeline_logging.logger.info("Creating Weather API client")
    weather_api_client = WeatherApiClient(api_key=API_KEY)
    pipeline_logging.logger.info("Extracting data from Weather API and CSV files")
    df_weather = extract_weather(weather_api_client=weather_api_client, city_reference_path="etl_project/data/australian_capital_cities.csv")
    df_population = extract_population(population_reference_path="etl_project/data/australian_city_population.csv")
    pipeline_logging.logger.info("Transforming dataframes")
    df = transform(df_weather=df_weather, df_population=df_population)
    pipeline_logging.logger.info("Loading data to postgres")
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
        Column("id", Integer, primary_key=True),
        Column("datetime", String, primary_key=True),
        Column("name", String),
        Column("temperature", Float),
        Column("population", Integer)
    )
    load(
        df=df, 
        postgresql_client=postgresql_client,
        table=table,
        metadata=metadata,
        load_method="upsert"
    )
    pipeline_logging.logger.info("Pipeline run successful")
