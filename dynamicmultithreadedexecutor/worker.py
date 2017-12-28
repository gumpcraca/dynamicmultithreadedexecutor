# outside imports
from six.moves.queue import Queue, Empty
import six
import threading
import logging
import traceback

# internal imports
from .utils import get_num_input_vars
from .exceptions import KillExecution

LOGGER = logging.getLogger(__name__)

def worker(inq, outq, deathq, worker_function, kill_boolean):
    """
    Worker function that is called once per item in inq but in a multithreaded manner

    Worker functions are spun up/down as needed based on how many threads the main dynamicmultithreaded object thinks we should have

    :rtype : None, although into outq we inject whatever is retruned from the worker_function, if worker_function raises an exception a dict with keys "exception" and "traceback" are returned

    :param inq: queue we will pull from to run worker_function
    :param outq: output queue we will put into with results from worker_function
    :param deathq: we pull from here every execution, if we get something we return and therefore kill off our thread
    :param worker_function: user provided function to execute
    :param kill_boolean: allows us to kill the whole execution from the worker raising a KillExecution exception

    :type inq: Queue
    :type outq: Queue
    :type deathq: Queue
    :type worker_function: callable
    :type kill_boolean: bool
    """
    assert isinstance(inq, Queue)
    assert isinstance(outq, Queue)
    assert isinstance(deathq, Queue)
    assert callable(worker_function)
    assert isinstance(kill_boolean, bool)

    LOGGER.info("spinning up thread: {}".format(threading.current_thread().name))
    
    if get_num_input_vars(worker_function) != 1:
        # Set our flag to kill all the things
        kill_boolean = True
        raise RuntimeError("worker function must take in at least one arg!")

    while True:
        # check the Queue to see if we should die
        try:
            _ = deathq.get(block=False)
        except Empty:
            pass
        else:
            # we didn't get an Empty exception, that means we need to die
            LOGGER.info("spinning down thread: {} - got a death threat from deathq".format(threading.current_thread().name))
            return
            
        if kill_boolean:
            LOGGER.info("spinning down thread: {} - got a death threat from kill_boolean".format(threading.current_thread().name))
            return
        
        # get work to do
        try:
            itm_to_run = inq.get(timeout=5)
        except Empty:
            LOGGER.info("inQ is empty, thread {} returning".format(threading.current_thread().name))
            return

        # Do work son!
        try:
            response = worker_function(itm_to_run)
            
            output = {
                "execution_success":True,
                "task_output":response
            }
        except KillExecution:
            LOGGER.warning("we got a KillExecution exception, killing off our execution and returning")
            kill_boolean = True
            return
        except Exception as e:
            tb = traceback.format_exec()
            output = {
                "execution_success":False,
                "exception_message":str(e),
                "traceback":tb
            }

        # dump our output into the outq
        outq.put(output)
        LOGGER.debug("work is done, putting into outq!")