# public imports
from six.moves.queue import Queue, Empty
import six
import logging

# internal imports
from .utils import get_num_input_vars

LOGGER = logging.getLogger(__name__)


def finisher(outq, output_queue_handler, kill_boolean):
    """
    execute output_func one time per item in outq
    output_func will be provided an item from outq and only that

    this function is executed in it's own thread but only single threaded
    :param outq: output queue that we will pull from
    :param output_queue_handler: function to run on every item in outq
    :param kill_boolean: set by worker thread indicates we should all die

    :type outq: Queue
    :type output_queue_handler: callable
    :type kill_boolean: bool
    """
    assert isinstance(outq, Queue)
    assert callable(output_queue_handler)
    assert isinstance(kill_boolean, bool)
    
    if get_num_input_vars(output_queue_handler) != 1:
        # TODO: Need a sentinel that will kill everything
        kill_boolean = True
        raise RuntimeError("output_queue_handler function must take in at least one arg!")
    
    while True:
        if kill_boolean:
            LOGGER.warning("Got a death threat from kill_boolean, quitting")
            return
            
        # this will block forever
        output_var = outq.get()

        # This is our death signal, could use a sentinel here, but seemed like overkill for just this one thread
        if output_var == 'DIE DIE DIE':
            LOGGER.warning("Finisher queue recieved death threat, quitting - if this didn't happen at the end of the program there's a problem")
            # Need to mark execution as complete!
            return
        
        output_queue_handler(output_var)

    raise RuntimeError("We should never get here, somehow we exited our while loop")