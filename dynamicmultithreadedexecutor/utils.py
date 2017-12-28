# public imports
import six
import inspect

def get_num_input_vars(callable_obj):
    """
    Return the number of args a function takes in
    """
    assert callable(callable_obj), "object must be callable"
    args = inspect.getfullargspec(callable_obj).args
    specials = ["self"]
    args = [a for a in args if a not in specials]
    return len(args)
    

def get_input_vars(func, possible_inputs, check_compliance = True):
    """
    Get the input variables that a function is lookign for. note: this does not work well when users use something other than *args and **kwargs for expansions

    :rtype : dict of kwarg inputs for function

    :type check_compliance: bool
    :type func: function
    :type possible_inputs: dict

    :param func: function to check inputs of
    :param possible_inputs: dict to check
    :return: inputs to provde to function using kwargs
    """
    #TODO: Should look into using functools.partial instead
    
    assert callable(func)
    assert isinstance(possible_inputs, dict)

    asked_for =  func.__code__.co_varnames[:func.__code__.co_argcount]

    # the number of default variables that are present
    num_defaults = -len(func.__defaults__) if func.__defaults__ else None
    required = asked_for[:num_defaults]

    # get what we can give
    applicable_vars = {k:v for k,v in six.iteritems(possible_inputs) if k in asked_for}

    if set(required) != set(applicable_vars) and check_compliance:
        # looks like we aren't going to provide enough variables for this to be successful
        raise RuntimeError("looks like function: {} has required vars: {} but we can only provide: {}".format(func.__code__.co_name,", ".join(required), ", ".join(six.iterkeys(applicable_vars))))

    return applicable_vars
