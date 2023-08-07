# Instruction

Now that you have successfully refactored your code. 

Let's extend our `PostgreSqlClient` class by adding the following methods to it: 

- `insert`: inserts all data provided in a list of dictionaries to the database and specified `table` and `metadata` object. This method should create the table if it doesn't already exist. 
- `overwrite`: overwrite all data provided in a list of dictionaries to the database and specified `table` and `metadata` object. This method should create the table if it doesn't already exist. 
- `upsert`: upsert all data provided in a list of dictionaries to the database and specified `table` and `metadata` object. This method should create the table if it doesn't already exist. 

Now, we can modify the `load` function in `assets` to take in a new argument for `load_method` which calls the different load methods available in our `PostgreSqlClient` class. 
