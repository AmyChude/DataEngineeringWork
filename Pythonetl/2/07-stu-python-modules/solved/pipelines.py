from dotenv import load_dotenv
import os 
from assets import extract, extract_exchange_codes, transform, load

if __name__ == "__main__":
    load_dotenv()
    API_KEY_ID = os.environ.get("API_KEY_ID")
    API_SECRET_KEY = os.environ.get("API_SECRET_KEY")
    DB_USERNAME = os.environ.get("DB_USERNAME")
    DB_PASSWORD = os.environ.get("DB_PASSWORD")
    SERVER_NAME = os.environ.get("SERVER_NAME")
    DATABASE_NAME = os.environ.get("DATABASE_NAME")
    df_alpaca_markets = extract(stock_ticker="tsla", start_date="2020-01-01", end_date="2020-01-03", api_key_id=API_KEY_ID, api_secret_key=API_SECRET_KEY)
    df_exchange_codes = extract_exchange_codes("data/exchange_codes.csv")
    df_exchange = transform(df=df_alpaca_markets, df_exchange_codes=df_exchange_codes)
    load(df_exchange, db_user=DB_USERNAME, db_password=DB_PASSWORD, db_server_name=SERVER_NAME, db_database_name=DATABASE_NAME)
