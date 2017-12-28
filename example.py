from dynamicmultithreadedexecutor import execute_dynamic_multithreaded_task
from dynamicmultithreadedexecutor.exceptions import KillExecution

from random import randrange
import logging
import time
from functools import partial


FORMAT = '%(asctime)s - %(levelname)s - %(message)s'
logging.basicConfig(format=FORMAT)

logging.getLogger().setLevel(logging.DEBUG)
LOGGER = logging.getLogger(__name__)


# callables/functions provided can be as simple as a regular function
# but we're going to demo using a partial here instead
def thread_checker_func(start, end):
    return randrange(start, end)

thread_checker = partial(thread_checker_func, 5, 10)

# you can also pass in a callable class if you'd like to have some additional variables provided to the worker
class worker():
    start = 5
    end = 10
    def __call__(self, itm):
        # do work here!
        LOGGER.info("handling itm: %s", itm)
        if itm == 25:
            raise KillExecution("DEATH!") # this will globally stop the execution of the multithreaded executor
        time.sleep(randrange(self.start, self.end))

# finally you could pass in a class function rather than the whole class, in this example we'll pass in def run
class output_queue_handler():
    def __init__(self, itm, mongo_obj, bdb_obj):
        self.itm = itm
        self.mongo_obj = mongo_obj
        self.bdb_obj = bdb_obj
        
    def run(self, itm):
        LOGGER.info("writing %s!", itm)

iterable = range(100)
poll_period = 20

LOGGER.info("STARTING UP!")
execute_dynamic_multithreaded_task(iterable, thread_checker, poll_period, worker(), output_queue_handler(1,2,3).run)

LOGGER.info("ENDED!")
