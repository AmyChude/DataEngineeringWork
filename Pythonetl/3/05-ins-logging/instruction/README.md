# Python Logging 

## Concept 

Now that we have a good structure going, let's look at a concept that will prove very helpful when we are looking to run our ETL jobs in production, and that is logging. 

Because we have moved away from Jupyter Notebooks, we are not able to interactively understand what is going on with our code anymore. One way to achieve this is to use lots of python `print()` statements in our python file. 

But there is a better way, and that is to use python `logging()`. Logging offers us features such as being able to control different levels of printing e.g. WARNING, ERROR, INFO, and also print a neatly formatted message into the terminal, that looks something like: 

```
2023-07-29 22:08:23,107 - weather - INFO - Starting pipeline run
2023-07-29 22:08:23,107 - weather - INFO - Getting pipeline environment variables
2023-07-29 22:08:23,107 - weather - INFO - Creating Weather API client
2023-07-29 22:08:23,107 - weather - INFO - Extracting data from Weather API and CSV files
2023-07-29 22:08:27,678 - weather - INFO - Transforming dataframes
2023-07-29 22:08:27,686 - weather - INFO - Loading data to postgres
2023-07-29 22:08:27,764 - weather - INFO - Pipeline run successful
```

## Implementation 

We are going to use the Python [Logging](https://docs.python.org/3/library/logging.html) module. Have a look at the docs. 

There are different levels we can log information for, which the docs will explain further: 
- debug
- info
- warning
- error 
- critical 

We can easily log a statement, similar to `print()` by doing: 

```python
import logging
logging.warning('Watch out!')  # will print a message to the console
logging.info('I told you so')  # will not print anything unless the configured logging level is changed 
```

We can change the logging level to also print `INFO` logs by doing: 

```python
import logging 
logging.basicConfig(level=logging.INFO)
```

Let's take a deeper dive into logging now. 
