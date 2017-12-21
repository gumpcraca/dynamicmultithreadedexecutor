# outside imports
from six.moves.queue import Queue, Empty
import six
import threading
import logging
import traceback

# internal imports
from utils import get_input_vars

LOGGER = logging.getLogger(__name__)

def worker(inq, outq, deathq, worker_function,common_kwargs):
    """
    Worker function that is called once per item in inq but in a multithreaded manner

    Worker functions are spun up/down as needed based on how many threads the main dynamicmultithreaded object thinks we should have

    :rtype : None, although into outq we inject whatever is retruned from the worker_function, if worker_function raises an exception a dict with keys "exception" and "traceback" are returned

    :param inq: queue we will pull from to run worker_function
    :param outq: output queue we will put into with results from worker_function
    :param deathq: we pull from here every execution, if we get something we return and therefore kill off our thread
    :param worker_function: user provided function to execute
    :param common_kwargs: additional args the worker_function may or may not want

    :type inq: Queue
    :type outq: Queue
    :type deathq: Queue
    :type worker_function: FunctionType
    """
    assert isinstance(inq, Queue)
    assert isinstance(outq, Queue)
    assert isinstance(deathq, Queue)
    assert isinstance(worker_function, six.types.FunctionType)
    assert common_kwargs is None or isinstance(common_kwargs, dict)

    LOGGER.info("spinning up thread: {}".format(threading.current_thread().name))

    spoofed_args = common_kwargs

    func_args = worker_function.__code__.co_varnames[:worker_function.__code__.co_argcount]
    if not func_args:
        # TODO: Need a sentinel that will kill everything
        raise RuntimeError("worker function must take in at least one arg!")

    first_arg_name = func_args[0]
    spoofed_args[first_arg_name] = None # assuming first arguement is for the item from iterable and can be called whatever
    task_inputs = get_input_vars(worker_function, spoofed_args)

    while True:
        # check the Queue to see if we should die
        try:
            _ = deathq.get(block=False)
        except Empty:
            pass
        else:
            # we didn't get an Empty exception, that means we need to die
            LOGGER.info("spinning down thread: {}".format(threading.current_thread().name))
            return

        # get work to do
        try:
            itm_to_run = inq.get(timeout=5)
        except Empty:
            LOGGER.info("inQ is empty, thread {} returning".format(threading.current_thread().name))
            return

        # Do work son!
        try:
            task_inputs[first_arg_name] = itm_to_run
            output = worker_function(**task_inputs)
        except Exception as e:
            tb = traceback.format_exec()
            output = {"exception":str(e),
                      "traceback":tb
                      }

        # dump our output into the outq
        outq.put(output)
        LOGGER.debug("work is done, putting into outq!")