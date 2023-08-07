import logging 
import time 
import schedule 
from io import StringIO
import sys

def task_1():
    
    logger = logging.getLogger("task_1")
    logger.setLevel(logging.INFO)
    file_handler = logging.FileHandler("task_1.log")
    file_handler.setLevel(logging.INFO)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)
    logger.info("Doing task 1")

def task_2():
    logger = logging.getLogger("task_2")
    logger.setLevel(logging.INFO)
    file_handler = logging.FileHandler("task_2.log")
    file_handler.setLevel(logging.INFO)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)
    logger.info("Doing task 2")

schedule.every(3).seconds.do(task_1)
schedule.every(5).seconds.do(task_2)

while True: 
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    schedule.run_pending()
    time.sleep(1)
