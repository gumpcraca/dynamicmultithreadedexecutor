class SuccessfulResult():
    def __init__(self, queue_item, task_output):
        """
        Container for storage of output from a successful run of the client worker method.
        Worker will return either this or ExceptionResult for every execution

        :param queue_item: original item ran through system
        :param task_output: the returned value from the task - None if exception occurred
        """
        self.execution_success = True
        self.queue_item = queue_item
        self.task_output = task_output

class ExceptionResult():
    def __init__(self, queue_item, exception_message=None, traceback=None):
        """
        Container for storage of output from a successful run of the client worker method.
        Worker will return either this or SuccessfulResult for every execution

        :param queue_item: original item ran through system
        :param exception_message: str, stringified exception message
        :param traceback: str, full traceback of exception
        """
        self.queue_item = queue_item
        self.exception_message = exception_message
        self.traceback = traceback
        self.execution_success = False