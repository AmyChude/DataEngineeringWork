# Instruction

As you were working through the previous exercise of breaking down code into Python modules, what problems did you notice with that approach? 

Well, one of the issues with functional programming is that you tend to have many variables being passed into functions. For example, when calling the `load` function, we had to pass in arguments for the server name, database name, username, password, port number, the dataframe, and so on. Next, within the `load` function, we had to also call `get_database_engine` and pass those same variables to get the database engine. 

And if we were to change the variables required by `get_database_engine`, then we need to change the variables passed into its parent function `load`. 

A great way to reduce the amount of variables being passed in from one function to another is through a combination of object oriented programming and dependency injection. 

Let's take a look at an example of how this works. 

```
.
├── etl_project
│   ├── __init__.py
│   ├── assets
│   │   ├── __init__.py
│   │   └── weather.py
│   ├── connectors
│   │   ├── __init__.py
│   │   ├── postgresql.py
│   │   └── weather_api.py
│   ├── data
│   │   ├── australian_capital_cities.csv
│   │   └── australian_city_population.csv
│   └── pipelines
│       └── weather.py
```
