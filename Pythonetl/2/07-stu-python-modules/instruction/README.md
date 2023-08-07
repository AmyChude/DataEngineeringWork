# Instruction 

Break down your code into separate logical folders and files so that the code becomes more maintainable and extensible. 

A good general structure to use for ETL pipelines is: 
- Assets: Code relating to individual ETL steps. 
- Connectors: Code that supports interacting with a data source such as an API, a database, or a file system. 
- Pipelines: Code that stitches everything together into an ETL pipeline. 

```
├── assets.py
├── connectors.py
├── data
│   └── exchange_codes.csv
├── pipelines.py
```
