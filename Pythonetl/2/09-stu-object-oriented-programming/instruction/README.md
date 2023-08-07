# Instruction

Refactor your project to make use of Python folder modules. 


```
.
├── etl_project
│   ├── __init__.py
│   ├── assets
│   │   ├── __init__.py
│   │   └── alpaca_markets.py
│   ├── connectors
│   │   ├── __init__.py
│   │   ├── alpaca_markets.py
│   │   └── postgresql.py
│   ├── data
│   │   └── exchange_codes.csv
│   └── pipelines
│       ├── __init__.py
│       └── alpaca_markets.py
└── .env
```

Refactor your code to apply object oriented programming and dependency injection. 

Create the following classes: 
- `AlpacaMarketsApiClient`
- `PostgreSqlClient`

Refactor the following functions to use those classes with dependency injection: 
- `extract` 
- `load`
