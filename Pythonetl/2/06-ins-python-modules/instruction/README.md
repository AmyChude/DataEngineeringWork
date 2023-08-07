# Instruction 

Having all code in a single Python file can be very lengthy and hard to understand the structure. 

It can help to break code down into separate logical folders and files so that the code becomes more maintainable and extensible. 

A good general structure to use for ETL pipelines is: 
- Assets: Code relating to individual ETL steps. 
- Connectors: Code that supports interacting with a data source such as an API, a database, or a file system. 
- Pipelines: Code that stitches everything together into an ETL pipeline. 

```
├── assets.py
├── connectors.py
├── data
│   ├── australian_capital_cities.csv
│   ├── australian_city_population.csv
├── pipelines.py
```

After breaking down our code into separate files, we can import python files by treating them as modules. 

Let's see how it's done! 

