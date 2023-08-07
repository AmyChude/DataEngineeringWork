# Multi Day ETL 

The Alpaca API only allows each request to return results for a specified day. Therefore, if we want to retrieve results over multiple days, we need to make multiple requests. 

Let's make use a function that returns a date list in your `Extract` logic to extract data for multiple days. 


## Task

1. A helper function called `_generate_datetime_ranges()` has been implemented for you. Read the function docstring to understand what the function is doing. 

2. Modify your `requests` extraction logic to loop over the date list returned from `_generate_datetime_ranges()`. 

