from dotenv import load_dotenv
import os 
from etl_project.assets.alpaca_markets import extract_alpaca_markets, extract_exchange_codes, transform, load
from etl_project.connectors.alpaca_markets import AlpacaMarketsApiClient
from etl_project.connectors.postgresql import PostgreSqlClient
from sqlalchemy import Table, Column, Integer, String, MetaData, Float
from etl_project.assets.pipeline_logging import PipelineLogging

if __name__ == "__main__":
    pipeline_logging = PipelineLogging(pipeline_name="alpaca_markets", log_folder_path="etl_project/logs")
    load_dotenv()
    pipeline_logging.logger.info("Starting pipeline run")
    pipeline_logging.logger.info("Getting pipeline environment variables")
    API_KEY_ID = os.environ.get("API_KEY_ID")
    API_SECRET_KEY = os.environ.get("API_SECRET_KEY")
    DB_USERNAME = os.environ.get("DB_USERNAME")
    DB_PASSWORD = os.environ.get("DB_PASSWORD")
    SERVER_NAME = os.environ.get("SERVER_NAME")
    DATABASE_NAME = os.environ.get("DATABASE_NAME")
    PORT = os.environ.get("PORT")
    pipeline_logging.logger.info("Creating Alpaca Markets API client")
    alpaca_markets_client = AlpacaMarketsApiClient(api_key_id=API_KEY_ID, api_secret_key=API_SECRET_KEY)
    pipeline_logging.logger.info("Extracting data from Alpaca API and CSV files")
    df_alpaca_markets = extract_alpaca_markets(alpaca_markets_client=alpaca_markets_client, stock_ticker="tsla", start_date="2020-01-01", end_date="2020-01-03")
    df_exchange_codes = extract_exchange_codes("etl_project/data/exchange_codes.csv")
    pipeline_logging.logger.info("Transforming dataframes")
    df_exchange = transform(df=df_alpaca_markets, df_exchange_codes=df_exchange_codes)
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
        "stock_price_tesla", metadata, 
        Column("id", Integer, primary_key=True),
        Column("exchange", String, primary_key=True),
        Column("timestamp", String, primary_key=True),
        Column("price", Float),
        Column("size", Integer)
    )
    load(
        df=df_exchange,
        postgresql_client=postgresql_client, 
        table=table, 
        metadata=metadata,
        load_method="upsert"
    )
    pipeline_logging.logger.info("Pipeline run successful")
