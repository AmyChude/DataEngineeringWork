# Extracting data

## Task

Now it is your turn. 

You are going to be extracting data from the Alpaca API. 

First you will need to create an Alpaca API account. Please create an account [here](https://app.alpaca.markets/signup).

After signing up, please refer to the docs [here](https://alpaca.markets/docs/market-data/getting-started/#creating-an-alpaca-account-and-finding-your-api-keys) to retrieve your API key (scroll to "Creating an Alpaca Account and Finding Your API Keys"). 

Once you have your API keys, you can now continue with the rest of your activity and write HTTP requests to extract your data. 

The important documentation you will need to reference while performing this task is listed below: 
- [API usage docs](https://alpaca.markets/docs/api-references/market-data-api/stock-pricing-data/historical/)
- [API auth example](https://alpaca.markets/docs/api-references/trading-api/)

Please extract data from the Trades API `/v2/stocks/{symbol}/trades` for the following date range: 
- `start_date: 2020-01-01T00:00:00.00Z`
- `end_date: 2020-01-02T00:00:00.00Z`

Store the extracted data into a Pandas DataFrame. 

Some code snippets below which might help: 


```python
# to authenticate to the api, you will need to use the APCA-API-KEY-ID and APCA-API-SECRET-KEY fields. 
# for example:
headers = {
    "APCA-API-KEY-ID": "<your_api_key_id>",
    "APCA-API-SECRET-KEY": "<your_api_secret_key>"
}   

# and in the request.get() method, you will have to use: 
requests.get(url=url, params=params, headers=headers)

```

