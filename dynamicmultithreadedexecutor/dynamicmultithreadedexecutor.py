# public imports
import logging
import time
import six
import threading
from six.moves.queue import Queue
import datetime
import collections

# internal imports
from finisher import finisher
from worker import worker
from utils import get_input_vars

LOGGER = logging.getLogger(__name__)


# TODO: change DIE DIE DIE to be a sentinel
# TODO: Need ability of worker thread to notify main thread to stop - seems like a queue is the right way to go, or maybe a lock/event/semaphore


def execute_dynamic_multithreaded_task(iterable, common_kwargs, thread_checker_func, poll_period, worker_function, output_queue_handler, on_start=None, on_finish=None):
    """
    Execute a function for every item in iterable with a dynamic number of threads as defined by the return of thread_checker_func

    :type iterable: any iterable
    :type common_kwargs: dict or None
    :type thread_checker_func: function with zero parameters or items from common_kwargs
    :type poll_period: int
    :type worker_function: function with at least 1 parameter
    :type output_queue_handler: function with at least 1 parameter
    :type on_start: None or function, function must return none or dict of variables to mutate
    :type on_finish: None or function, function returns are not used

    :param iterable: Iterable to pass into worker_function
    :param common_kwargs: additional kwargs to be provided to output_queue_handler as well as worker_function
    :param thread_checker_func: function that accepts no args and will return int for # of threads we should run
    :param poll_period: how often (in sec) we will run thread_checker_func
    :param worker_function: function that will be run multi-threaded and once per item in file_list
    :param output_queue_handler: consume things that worker_function returns. this will run single threaded, once per execution
    :param on_start: this will be run once at the very start of the execution before any additional threads are spun up. it will be provided iterable and common_kwargs, it will be allowed to mutate iterable or anything in common_kwargs
    :param on_finish: this will be run once at the very end of the execution after all other threads are spun down.

    :rtype : None - output_queue_handler should handle all output functionality
    """
    LOGGER.info("starting dynamic multithreaded execution")

    # Type checking on all inputs
    assert isinstance(iterable, collections.Iterable)
    assert isinstance(common_kwargs, dict)
    assert isinstance(thread_checker_func, six.types.FunctionType)
    assert isinstance(poll_period, six.integer_types)
    assert isinstance(worker_function, six.types.FunctionType)
    assert isinstance(output_queue_handler, six.types.FunctionType)
    assert on_start is None or isinstance(on_start, six.types.FunctionType)
    assert on_finish is None or isinstance(on_finish, six.types.FunctionType)

    LOGGER.info("all assertions passed")
    
    # Validate function inputs are good (check to ensure they accept at least one variable
    if worker_function.__code__.co_argcount < 1:
        raise RuntimeError("worker_function must accept at least one input variable")

    if output_queue_handler.__code__.co_argcount < 1:
        raise RuntimeError("output_queue_handler must accept at least one input variable")

    if worker_function.__code__.co_varnames[0] in common_kwargs:
        raise RuntimeError("worker_functions's first arg must be whatever comes from iterable and not a key in common_args")

    if output_queue_handler.__code__.co_varnames[0] in common_kwargs:
        raise RuntimeError("output_queue_handler's first arg must be whatever comes from iterable and not a key in common_args")

    LOGGER.info("functions appear to have ok inputs")

    # TODO: Still need the ability to kill the execution from the finisher queue since it's who knows about all the crashes

    # prep the thread-wide variables
    inq = Queue() # queue full of filenames
    outq = Queue() # queue we will write from
    deathq = Queue() # queue to tell the next thread that's done with execution to die

    # Execute our on_start function
    if on_start:
        LOGGER.info("on_start was provided, getting inputs")
        all_inputs = common_kwargs
        all_inputs["iterable"] = iterable
        input_vars = get_input_vars(on_start, all_inputs)

        LOGGER.info("inputs obtained, going to provide the following variables: {}".format(", ".join(six.iterkeys(input_vars))))
        mutated = on_start(**input_vars)

        LOGGER.info("on_start complete")

        # allow for any  mutation
        if mutated:
            LOGGER.info("on_start returned something, going to try and mutate existing iterable or common_kwargs")

            if not isinstance(mutated, dict):
                raise RuntimeError("on_start must return nothing or dict")

            if "iterable" in mutated:
                LOGGER.info("mutating iterable")
                iterable = mutated["iterable"]

            del mutated["iterable"]
            if len(mutated) > 0:
                for k,v in six.iteritems(mutated):
                    if k in common_kwargs:
                        LOGGER.info("mutating: {}".format(k))
                        common_kwargs[k] = v

            LOGGER.info("on_start mutation done")

    LOGGER.info("loading up inq")
    # Load up inq
    inq.queue.extend(iterable)

    thread_list = []

    # spin up our finisher thread
    LOGGER.info("starting up finisher thread")
    fin_thread = threading.Thread(target=finisher, kwargs={"outq":outq, "output_queue_handler":output_queue_handler,"common_kwargs":common_kwargs})
    fin_thread.start()

    # do all the executions, scaling up/down as needed
    LOGGER.info("getting thread_checker_func's input vars")
    thread_checker_func_vars = get_input_vars(thread_checker_func, common_kwargs)
    LOGGER.info("looks like thread_checker_func wants the following vars: {}".format(", ".join(six.iterkeys(thread_checker_func_vars))))
    LOGGER.info("entering infinite loop (until job is done)")

    while True:
        last_run = datetime.datetime.now()

        if not inq.empty():
            # get new target for our threads
            target_threads = thread_checker_func(**thread_checker_func_vars)

            # this could feasibly be done better, right now we are blocking until all deathq items are taken
            # we could do math and manage the deathq or spin up more threads based on that, which could make our deathq more accurate and less up / down
            # concern here is that this "control" algorithm get out of whack and vacillate up and down too much
            # Especially since we effect BDB Load

            # prob don't need this but doing it just in case
            thread_list = [t for t in thread_list if t.is_alive()]

            # spin up threads if need be
            while len(thread_list) < target_threads:
                LOGGER.debug("spinning up a new worker thread")
                base_kwargs = {"inq":inq,"outq":outq,"deathq":deathq,"worker_function":worker_function,"common_kwargs":common_kwargs}
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

                LOGGER.info("All threads have spun down")
                if on_finish:
                    LOGGER.info("running on_finish")
                    input_vars = get_input_vars(on_start, common_kwargs)
                    on_finish(**input_vars)

                LOGGER.info("All done! Returning!")
                return
            else:
                LOGGER.info("inq is empty, but looks like we still have {} threads running, we will wait until all threads complete".format(len(thread_list)))


        # only check for load every [poll_period] seconds
        while (datetime.datetime.now() - last_run).total_seconds() < poll_period:
            time.sleep(1)
