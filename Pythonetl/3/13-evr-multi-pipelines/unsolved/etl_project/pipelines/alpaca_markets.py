from dotenv import load_dotenv
import os
from etl_project.connectors.alpaca_markets import AlpacaMarketsApiClient
from etl_project.connectors.postgresql import PostgreSqlClient
from sqlalchemy import Table, MetaData, Column, Integer, String, Float
from etl_project.assets.alpaca_markets import (
    extract_alpaca_markets, 
    extract_exchange_codes,
    transform,
    load
)
from etl_project.assets.metadata_logging import MetaDataLogging, MetaDataLoggingStatus
from io import StringIO
import logging
import yaml 
from pathlib import Path
import schedule 
import time
from etl_project.assets.pipeline_logging import PipelineLogging

def pipeline(config: dict, pipeline_logging: PipelineLogging):
    pipeline_logging.logger.info("Starting pipeline run")
    # set up environment variables 
    pipeline_logging.logger.info("Getting pipeline environment variables")
    API_KEY_ID = os.environ.get("API_KEY_ID")
    API_SECRET_KEY = os.environ.get("API_SECRET_KEY")
    SERVER_NAME = os.environ.get("SERVER_NAME")
    DATABASE_NAME = os.environ.get("DATABASE_NAME")
    DB_USERNAME = os.environ.get("DB_USERNAME")
    DB_PASSWORD = os.environ.get("DB_PASSWORD")
    PORT = os.environ.get("PORT")
    
    pipeline_logging.logger.info("Creating Alpaca Markets API client")
    alpaca_markets_client = AlpacaMarketsApiClient(api_key_id=API_KEY_ID, api_secret_key=API_SECRET_KEY)
    
    # extract 
    pipeline_logging.logger.info("Extracting data from Alpaca API and CSV files")
    df_alpaca_markets = extract_alpaca_markets(
        alpaca_markets_client=alpaca_markets_client, 
        stock_ticker=config.get("stock_ticker"), 
        start_date=config.get("start_date"),
        end_date=config.get("end_date")
    )
    df_exchange_codes = extract_exchange_codes(exchange_codes_path=config.get("exchange_codes_path")) 
    # transform 
    pipeline_logging.logger.info("Transforming dataframes")
    df_transformed = transform(df=df_alpaca_markets, df_exchange_codes=df_exchange_codes)
    # load 
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
        f"alpaca_markets_trades_{config.get('stock_ticker').lower()}", metadata, 
        Column("id", Integer, primary_key=True),
        Column("timestamp", String, primary_key=True),
        Column("exchange", String, primary_key=True),
        Column("price", Float),
        Column("size", Integer)
    )
    load(
        df=df_transformed, 
        postgresql_client=postgresql_client,
        table=table,
        metadata=metadata,
        load_method="upsert"
    )
    pipeline_logging.logger.info("Pipeline run successful")
    
def run_pipeline_schedule(pipeline_name: str, postgresql_logging_client: PostgreSqlClient, pipeline_config: dict):
    pipeline_logging = PipelineLogging(pipeline_name=pipeline_name, log_folder_path=pipeline_config.get("config").get("log_folder_path"))
    metadata_logger = MetaDataLogging(
        pipeline_name=pipeline_name, 
        postgresql_client=postgresql_logging_client,
        config=pipeline_config.get("config")
    )
    try: 
        metadata_logger.log() # log start
        pipeline(config=pipeline_config.get("config"), pipeline_logging=pipeline_logging)
        metadata_logger.log(status=MetaDataLoggingStatus.RUN_SUCCESS, logs=pipeline_logging.get_logs()) # log end
        pipeline_logging.logger.handlers.clear()
    except BaseException as e:
        pipeline_logging.logger.error(f"Pipeline run failed. See detailed logs: {e}")
        metadata_logger.log(status=MetaDataLoggingStatus.RUN_FAILURE, logs=pipeline_logging.get_logs()) # log error
        pipeline_logging.logger.handlers.clear()

if __name__ == "__main__":
    # set up logging 
    # stream = StringIO()
    # logging.basicConfig(level=logging.INFO, stream=stream, format="[%(levelname)s][%(asctime)s][%(filename)s]: %(message)s") # format: https://docs.python.org/3/library/logging.html#logging.LogRecord

    # set up environment variables 
    load_dotenv()
    LOGGING_SERVER_NAME = os.environ.get("LOGGING_SERVER_NAME")
    LOGGING_DATABASE_NAME = os.environ.get("LOGGING_DATABASE_NAME")
    LOGGING_USERNAME = os.environ.get("LOGGING_USERNAME")
    LOGGING_PASSWORD = os.environ.get("LOGGING_PASSWORD")
    LOGGING_PORT = os.environ.get("LOGGING_PORT")
    
    postgresql_logging_client = PostgreSqlClient(
        server_name=LOGGING_SERVER_NAME,
        database_name=LOGGING_DATABASE_NAME,
        username=LOGGING_USERNAME,
        password=LOGGING_PASSWORD,
        port=LOGGING_PORT
    )

    # get config variables
    yaml_file_path = __file__.replace(".py", ".yaml")
    if Path(yaml_file_path).exists():
        with open(yaml_file_path) as yaml_file:
            pipeline_config = yaml.safe_load(yaml_file)
            PIPELINE_NAME = pipeline_config.get("name")
    else:
        raise Exception(f"Missing {yaml_file_path} file! Please create the yaml file with at least a `name` key for the pipeline name.")
    
    # set schedule 
    schedule.every(pipeline_config.get("schedule").get("run_seconds")).seconds.do(run_pipeline_schedule, pipeline_name=PIPELINE_NAME, postgresql_logging_client=postgresql_logging_client, pipeline_config=pipeline_config)

    while True: 
        schedule.run_pending()
        time.sleep(pipeline_config.get("schedule").get("poll_seconds"))
