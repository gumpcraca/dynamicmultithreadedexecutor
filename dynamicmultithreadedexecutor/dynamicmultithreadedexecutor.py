# public imports
import logging
import time
import six
import threading
from six.moves.queue import Queue
import datetime
import collections
import inspect

# internal imports
from .finisher import finisher
from .worker import worker
from .utils import get_num_input_vars

LOGGER = logging.getLogger(__name__)


# TODO: change DIE DIE DIE to be a sentinel
# TODO: Need ability of worker thread to notify main thread to stop - seems like a queue is the right way to go, or maybe a lock/event/semaphore


def execute_dynamic_multithreaded_task(iterable, thread_checker_func, poll_period, worker_function, output_queue_handler):
    """
    Execute a function for every item in iterable with a dynamic number of threads as defined by the return of thread_checker_func

    :type iterable: any iterable
    :type thread_checker_func: function with zero parameters and returns int of # of threads should be running
    :type poll_period: int
    :type worker_function: function with at least 1 parameter
    :type output_queue_handler: function with at least 1 parameter

    :param iterable: Iterable to pass into worker_function
    :param thread_checker_func: function that accepts no args and will return int for # of threads we should run
    :param poll_period: how often (in sec) we will run thread_checker_func
    :param worker_function: function that will be run multi-threaded and once per item in file_list
    :param output_queue_handler: consume things that worker_function returns. this will run single threaded, once per execution

    :rtype : None - output_queue_handler should handle all output functionality
    """
    LOGGER.info("starting dynamic multithreaded execution")

    # Type checking on all inputs
    assert isinstance(iterable, collections.Iterable)
    assert callable(thread_checker_func)
    assert isinstance(poll_period, six.integer_types)
    assert callable(worker_function)
    assert callable(output_queue_handler)

    LOGGER.info("all assertions passed, doing some checks on the callables passed in")
    
    # Validate function inputs are good (check to ensure they accept at least one variable
    if get_num_input_vars(worker_function) != 1:
        raise RuntimeError("worker_function must accept one and only one inputs")

    if get_num_input_vars(output_queue_handler) != 1:
        raise RuntimeError("output_queue_handler must accept one and only one inputs")
    
    if get_num_input_vars(thread_checker_func) != 0:
        raise RuntimeError("thread_checker_func must accept no inputs")
    
    LOGGER.info("callables appear to have ok inputs")

    # prep the thread-wide variables
    inq = Queue() # queue full of filenames
    outq = Queue() # queue we will write from
    deathq = Queue() # queue to tell the next thread that's done with execution to die
    kill_boolean = False
    
    LOGGER.info("loading up inq")
    # Load up inq
    inq.queue.extend(iterable)

    thread_list = []

    # spin up our finisher thread
    LOGGER.info("starting up finisher thread")
    fin_thread = threading.Thread(target=finisher, kwargs={"outq":outq, "output_queue_handler":output_queue_handler,"kill_boolean":kill_boolean})
    fin_thread.start()

    # do all the executions, scaling up/down as needed
    LOGGER.info("entering infinite loop (until job is done)")

    while True:
        last_run = datetime.datetime.now()
        if kill_boolean:
            # everything should spin down and die
            LOGGER.debug("kill_boolean is true, we are going to stop now!")
            return
            
        if not inq.empty():
            # get new target for our threads
            target_threads = thread_checker_func()

            # this could feasibly be done better, right now we are blocking until all deathq items are taken
            # we could do math and manage the deathq or spin up more threads based on that, which could make our deathq more accurate and less up / down
            # concern here is that this "control" algorithm get out of whack and vacillate up and down too much
            # Especially since we effect BDB Load

            # prob don't need this but doing it just in case
            thread_list = [t for t in thread_list if t.is_alive()]

            # spin up threads if need be
            while len(thread_list) < target_threads:
                LOGGER.debug("spinning up a new worker thread")
                base_kwargs = {"inq":inq,"outq":outq,"deathq":deathq,"worker_function":worker_function, "kill_boolean":kill_boolean}
                t = threading.Thread(target=worker, kwargs=base_kwargs)
                t.start()
                thread_list.append(t)

            # kill any extra threads
            thread_overage = len(thread_list) - target_threads
            for i in range(thread_overage):
                # kill em
                LOGGER.debug("sending death signal to deathq")
                deathq.put("DIE DIE DIE")

            # wait up to 10 min for deathq to be empty, then start forcibly killing threads
            # TODO: need to implement forcibly killing
            while not deathq.empty():
                time.sleep(1)

            # deathq is empty, which means we should have killed off however many threads we needed to
            # keeping this out of the if statement above in case we get exceptions in our child threads, we can spin up new workers
            thread_list = [t for t in thread_list if t.is_alive()]

            LOGGER.debug("Currently have {} threads running".format(len(thread_list)))

        else:
            # inq is empty, we need to see if we have any threads
            thread_list = [t for t in thread_list if t.is_alive()]
            if not thread_list:
                print("All worker threads are done, killing finisher thread")
                outq.put("DIE DIE DIE")

                # wait for finisher thread to die
                while fin_thread.is_alive():
                    print("finisher thread is still running, sleeping")
                    time.sleep(1)

                LOGGER.info("All threads have spun down, returning!")
                return
            else:
                LOGGER.info("inq is empty, but looks like we still have {} threads running, we will wait until all threads complete".format(len(thread_list)))


        # only check for load every [poll_period] seconds
        while (datetime.datetime.now() - last_run).total_seconds() < poll_period:
            time.sleep(1)
