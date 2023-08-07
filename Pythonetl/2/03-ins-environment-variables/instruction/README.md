# Instruction

An environment variable is a variable whose value is set outside the program, typically through functionality built into the operating system or microservice. An environment variable is made up of a name/value pair, and any number may be created and available for reference at a point in time.

Environment variables are often used to temporarily hold secrets, passwords, and API keys for use in an application. This way, the secrets don't need to be stored in plaintext in a git repository. 

To set an environment variable, you would run one of the following commands depending on your operating system: 

MacOS/Linux: 

```
export MY_API_KEY="123456789"
```

Windows:

```
set MY_API_KEY=123456789
```

To use an already set environment variable, you would do the following in a python file: 

```python
import os 

MY_API_KEY = os.environ.get("MY_API_KEY")
print(MY_API_KEY) # prints "123456789"
```

There are easier ways to manage environment variables by using a `.env` file. 

With a `.env` file, environment variables are set by doing: 

```
MY_API_KEY="123456789"
```

The `.env` file makes managing environment variables easy regardless of what operating system you use. 

To consume environment variables from a `.env` file, you would do the following in a python file: 

```python
from dotenv import load_dotenv
import os

load_dotenv() # this command loads the environment variables specified in the `.env` file
MY_API_KEY = os.environ.get("MY_API_KEY")
print(MY_API_KEY) # prints "123456789"
```