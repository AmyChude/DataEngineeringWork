from dotenv import load_dotenv
import os 
from assets import extract_city_population, extract_weather_data, transform, load

if __name__ == "__main__": 
    load_dotenv()
    API_KEY = os.environ.get("API_KEY")
    DB_USERNAME = os.environ.get("DB_USERNAME")
    DB_PASSWORD = os.environ.get("DB_PASSWORD")
    SERVER_NAME = os.environ.get("SERVER_NAME")
    DATABASE_NAME = os.environ.get("DATABASE_NAME")
    weather_data = extract_weather_data("data/australian_capital_cities.csv", api_key=API_KEY)
    df_population = extract_city_population("data/australian_city_population.csv")
    df = transform(weather_data=weather_data, df_population=df_population)
    load(df, db_user=DB_USERNAME, db_password=DB_PASSWORD, db_server_name=SERVER_NAME, db_database_name=DATABASE_NAME)
