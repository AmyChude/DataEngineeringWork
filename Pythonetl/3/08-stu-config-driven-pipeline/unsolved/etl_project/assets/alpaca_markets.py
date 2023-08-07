import pandas as pd 
from etl_project.connectors.alpaca_markets import AlpacaMarketsApiClient
from pathlib import Path
from sqlalchemy import Table, MetaData
from etl_project.connectors.postgresql import PostgreSqlClient
from datetime import datetime, timezone, timedelta

def _generate_datetime_ranges(
        start_date: str, 
        end_date: str, 
    ) -> list[dict[str, datetime]]:
    """ 
    Generates a range of datetime ranges. 
    
    Usage example: 
        _generate_datetime_ranges(start_date="2020-01-01", end_date="2020-01-03")

    Returns: 
            [
                {'start_time': datetime(2020, 1, 1, 0, 0, 0, tzinfo=timezone.utc), 'end_time': datetime(2020, 1, 2, 0, 0, 0, tzinfo=timezone.utc)}, 
                {'start_time': datetime(2020, 1, 2, 0, 0, 0, tzinfo=timezone.utc), 'end_time': datetime(2020, 1, 3, 0, 0, 0, tzinfo=timezone.utc)}
            ]
    
    Args: 
        start_date: provide a str with the format "yyyy-mm-dd"
        end_date: provide a str with the format "yyyy-mm-dd" 

    Returns: 
        A list of dictionaries with datetime objects 
    
    Raises:
        Exception when incorrect input date string format is provided. 
    """

    date_range = []
    if start_date is not None and end_date is not None: 
        raw_start_time = datetime.strptime(start_date, "%Y-%m-%d")
        raw_end_time = datetime.strptime(end_date, "%Y-%m-%d")
        start_time = datetime(
            year=raw_start_time.year, 
            month=raw_start_time.month, 
            day=raw_start_time.day, 
            hour=raw_start_time.hour,
            minute=raw_start_time.minute,
            second=raw_start_time.second,
            tzinfo=timezone.utc
        )
        end_time = datetime(
            year=raw_end_time.year, 
            month=raw_end_time.month, 
            day=raw_end_time.day, 
            hour=raw_end_time.hour,
            minute=raw_end_time.minute,
            second=raw_end_time.second,
            tzinfo=timezone.utc
        )
        date_range = [
            {
                "start_time": (start_time + timedelta(days=i)),
                "end_time": (start_time + timedelta(days=i) + timedelta(days=1)),
            }
        for i in range((end_time - start_time).days)]
    else: 
        raise Exception("Please provide valid dates `YYYY-MM-DD` for start_date and end_date.")
    return date_range  

def extract_alpaca_markets(
        alpaca_markets_client: AlpacaMarketsApiClient, 
        stock_ticker: str, 
        start_date: str,
        end_date: str
    )->pd.DataFrame:
    """
    Perform extraction using a filepath which contains a list of cities. 
    """
    
    data = []
    for dates in _generate_datetime_ranges(start_date=start_date, end_date=end_date):
        data.extend(alpaca_markets_client.get_trades(
            stock_ticker=stock_ticker,
            start_time=dates.get("start_time").isoformat(),
            end_time=dates.get("end_time").isoformat()
        ))
    
    df = pd.json_normalize(data=data, meta=["symbol"])
    return df

def extract_exchange_codes(
    exchange_codes_path: Path
) -> pd.DataFrame:
    """Extracts data from the exchange codes file"""
    df = pd.read_csv(exchange_codes_path)
    return df

def transform(
        df:pd.DataFrame, 
        df_exchange_codes:pd.DataFrame
    )->pd.DataFrame:
    """Performs transformation on dataframe produced from extract() function."""
    df_quotes_renamed = df.rename(columns={
        "i": "id",
        "t": "timestamp",
        "x": "exchange",
        "p": "price",
        "s": "size",
    })
    df_quotes_selected = df_quotes_renamed[['id', 'timestamp', 'exchange', 'price', 'size']]
    df_exchange = pd.merge(
        left=df_quotes_selected, 
        right=df_exchange_codes, 
        left_on="exchange", 
        right_on="exchange_code"
    ).drop(
        columns=["exchange_code", "exchange"]
    ).rename(
        columns={"exchange_name": "exchange"}
    )
    return df_exchange

def load(
        df: pd.DataFrame,
        postgresql_client: PostgreSqlClient, 
        table: Table, 
        metadata: MetaData, 
        load_method: str = "overwrite"
    ) -> None:
    """
    Load dataframe to a database.

    Args:
        df: dataframe to load
        postgresql_client: postgresql client
        table: sqlalchemy table
        metadata: sqlalchemy metadata
        load_method: supports one of: [insert, upsert, overwrite]
    """
    if load_method == "insert":
        postgresql_client.insert(
            data=df.to_dict(orient='records'),
            table=table,
            metadata=metadata
        )
    elif load_method == "upsert":
        postgresql_client.upsert(
            data=df.to_dict(orient='records'),
            table=table,
            metadata=metadata
        )
    elif load_method == "overwrite": 
        postgresql_client.overwrite(
            data=df.to_dict(orient='records'),
            table=table,
            metadata=metadata
        )
    else: 
        raise Exception("Please specify a correct load method: [insert, upsert, overwrite]")
