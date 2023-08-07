from dotenv import load_dotenv
import os 
from etl_project.assets.alpaca_markets import extract_alpaca_markets, extract_exchange_codes, transform, load
from etl_project.connectors.alpaca_markets import AlpacaMarketsApiClient
from etl_project.connectors.postgresql import PostgreSqlClient
from sqlalchemy import Table, Column, Integer, String, MetaData, Float
import logging
import yaml 
from pathlib import Path
from etl_project.assets.pipeline_logging import PipelineLogging
from etl_project.assets.metadata_logging import MetaDataLogging, MetaDataLoggingStatus

if __name__ == "__main__":
    load_dotenv()
    # get config variables
    yaml_file_path = __file__.replace(".py", ".yaml")
    if Path(yaml_file_path).exists():
        with open(yaml_file_path) as yaml_file:
            pipeline_config = yaml.safe_load(yaml_file)
            config = pipeline_config.get("config")
            PIPELINE_NAME = pipeline_config.get("name")
    else:
        raise Exception(f"Missing {yaml_file_path} file! Please create the yaml file with at least a `name` key for the pipeline name.")
    
    pipeline_logging = PipelineLogging(pipeline_name=pipeline_config.get("name"), log_folder_path=config.get("log_folder_path"))
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

    metadata_logger = MetaDataLogging(
        pipeline_name=PIPELINE_NAME, 
        postgresql_client=postgresql_logging_client,
        config=pipeline_config.get("config")
    )
    try: 
        metadata_logger.log() # log start
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
        df_alpaca_markets = extract_alpaca_markets(alpaca_markets_client=alpaca_markets_client, stock_ticker=config.get("stock_ticker"), start_date=config.get("start_date"), end_date=config.get("end_date"))
        df_exchange_codes = extract_exchange_codes(config.get("exchange_codes_path"))
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
        metadata_logger.log(status=MetaDataLoggingStatus.RUN_SUCCESS, logs=pipeline_logging.get_logs()) # log end
    except BaseException as e: 
        pipeline_logging.logger.error(f"Pipeline run failed. See detailed logs: {e}")
        metadata_logger.log(status=MetaDataLoggingStatus.RUN_FAILURE, logs=pipeline_logging.get_logs()) # log error
