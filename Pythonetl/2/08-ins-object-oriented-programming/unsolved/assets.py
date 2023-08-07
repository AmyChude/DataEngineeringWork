from connectors import get_weather_api, get_engine, write_to_database
import pandas as pd 


def extract_weather_data(csv_path: str, api_key: str) -> list[dict]: 
    # read list of cities
    df_cities = pd.read_csv(csv_path)
    df_cities.head()

    # request data for each city (json) and push to a list 
    weather_data = []
    for city_name in df_cities["city_name"]:
        data = get_weather_api(city_name=city_name, api_key=api_key)
        weather_data.append(data)

    return weather_data 

def extract_city_population(csv_path: str) -> pd.DataFrame: 
    return pd.read_csv(csv_path)

def transform(weather_data: list[dict], df_population: pd.DataFrame) -> pd.DataFrame:
    pd.options.mode.chained_assignment = None  # default='warn'
    # convert list into dataframe 
    df_weather_cities = pd.json_normalize(weather_data)

    # set city names to lowercase 
    df_weather_cities["city_name"] = df_weather_cities["name"].str.lower()

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

    return df_selected

def load(df: pd.DataFrame, db_user: str, db_password: str, db_server_name: str, db_database_name: str): 
    engine = get_engine(
        db_user=db_user, 
        db_password=db_password, 
        db_server_name=db_server_name, 
        db_database_name=db_database_name
    )
    write_to_database(df=df, engine=engine)
