from dotenv import load_dotenv
import os
import yaml 
from pathlib import Path
import schedule 
import time
from etl_project.connectors.postgresql import PostgreSqlClient
from importlib import import_module

if __name__ == "__main__":
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
            multi_pipeline_config = yaml.safe_load(yaml_file)
            print(multi_pipeline_config)
    else:
        raise Exception(f"Missing {yaml_file_path} file! Please create the yaml file with at least a `name` key for the pipeline name.")
    
    for pipeline_config in multi_pipeline_config.get("pipelines"):
        pipeline_name = pipeline_config.get("name")
        module = import_module(name=f".{pipeline_name}", package="etl_project.pipelines")
        schedule.every(pipeline_config.get("schedule").get("run_seconds")).seconds.do(
            module.run_pipeline, 
            pipeline_name=pipeline_name, 
            postgresql_logging_client=postgresql_logging_client, 
            pipeline_config=pipeline_config
        )

    while True: 
        schedule.run_pending()
        time.sleep(multi_pipeline_config.get("poll_seconds"))
