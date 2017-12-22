class KillExecution(Exception):
    """
    This is caught in our worker funciton to kill the rest of all our worker functions
    the rest of our workers will gracefully exit along with all other threads
    """
    pass