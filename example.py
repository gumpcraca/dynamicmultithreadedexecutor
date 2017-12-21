from dynamicmultithreadedexecutor import execute_dynamic_multithreaded_task

from random import randrange
import logging
import time

FORMAT = '%(asctime)s - %(levelname)s - %(message)s'
logging.basicConfig(format=FORMAT)

logging.getLogger().setLevel(logging.DEBUG)
LOGGER = logging.getLogger(__name__)

def thread_checker_func(start, end):
    return randrange(start, end)
    
def worker_function(itm, mongo_obj):
    time.sleep(randrange(3,5))
    return "{}.done".format(itm)

def output_queue_handler(itm, mongo_obj, bdb_obj):
    print("writing {}!".format(itm))
    return

common_kwargs = {
    "start":5, 
    "end":10,
    "mongo_obj":-1,
    "bdb_obj":-1
    }

def on_start():
    LOGGER.info("hey! This is on_start!")

def on_finish():
    LOGGER.info("hey! This is on_finish!")
    
iterable = range(100)
poll_period = 20

LOGGER.info("STARTING UP!")
execute_dynamic_multithreaded_task(iterable, common_kwargs, thread_checker_func, poll_period, worker_function, output_queue_handler, on_start, on_finish)
LOGGER.info("ENDED!")