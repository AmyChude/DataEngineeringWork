# Instruction

Create a schedule for your ETL pipeline using `schedule`. 

Refer to the [schedule](https://schedule.readthedocs.io/en/stable/) docs on usage instructions. 

Here is a code example: 

```python
import schedule
import time

def job():
    print("I'm working...")

schedule.every(10).minutes.do(job)
schedule.every().hour.do(job)
schedule.every().day.at("10:30").do(job)
schedule.every().monday.do(job)
schedule.every().wednesday.at("13:15").do(job)
schedule.every().day.at("12:42", "Europe/Amsterdam").do(job)
schedule.every().minute.at(":17").do(job)

while True:
    schedule.run_pending()
    time.sleep(1)
```

A schedule is set by doing `schedule.every(X).minute.do(job)`. When the schedule needs to execute, it will run the `job()` function. 

The code above polls every second using `time.sleep(1)` to check if the job needs to be run.

