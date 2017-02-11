import functools
import logging
import time

logger = logging.getLogger(__name__)

EXCEPTIONS_ARG = "_exceptions"
INITIAL_SLEEP_ARG = "_initial_sleep"
TOTAL_RETRIES_ARG = "_total_retries"
SLEEP_FUNCTION_ARG = "_sleep_function"
CLEANUP_FUNCTION_ARG = "_cleanup_function"
IS_RETRY_OK_FUNCTION_ARG = "_is_retry_ok_function"

DEFAULT_EXCEPTIONS = (Exception,)
DEFAULT_INITIAL_SLEEP = 1
DEFAULT_TOTAL_RETRIES = 5
DEFAULT_SLEEP_FUNCTION = lambda t, attempt: t * 2
DEFAULT_CLEANUP_FUNCTION = None
DEFAULT_IS_RETRY_OK_FUNCTION = lambda error: True


def with_backoff(
        func=None,
        exceptions=DEFAULT_EXCEPTIONS,
        initial_sleep=DEFAULT_INITIAL_SLEEP,
        retries=DEFAULT_TOTAL_RETRIES,
        sleep_function=DEFAULT_SLEEP_FUNCTION,
        cleanup_function=DEFAULT_CLEANUP_FUNCTION,
        is_retry_ok_function=DEFAULT_IS_RETRY_OK_FUNCTION):
    """
    Decorator that retries a function with exponential backoff.

    Args:

        func (function): The function to decorate. Usually this is not needed;
            with_backoff is generally used as a decorator.
        exceptions (Exception or tuple of Exceptions): The exception or
            exceptions that will trigger a retry. Any other exceptions will
            raise immediately.
        initial_sleep (int): The initial sleep time after a failure.
        retries (int): The total number of times to retry.
        sleep_function (function(int, int) -> int): A function that increases
            sleep time each run. This function needs to take two integer
            arguments (time slept last attempt, attempt number) and return an
            integer. By default we simply multiply "t" by two every time.
        cleanup_function (function()): This is a function that takes no
            arguments. If provided, it will be run after each failure. It is
            intended to clean up after a failed call to the decorated
            function (temp files on disk, database connections, etc.)
        is_retry_ok_function (function(Exception) -> bool): This function takes
            a single argument, the exception raised during the latest retry of
            the decorated function. This function evaluates the raised
            exceptions and determines if the retry process should be
            terminated immediately or should continue normally. If this
            function returns True, the retry process will continue. If False is
            returned, the retry process will end immediately and the exception
            will be raised.

    Returns:

        A decorator that can be used to decorate unreliable functions (if
        called with no "func" argument

        A decorated function (if used as a decorator)

    All keyword arguments are optional. Set `exceptions` to limit which
    exceptions retry, by default it will retry on all exceptions.

    If you have an unreliable Python function or method, it might be useful to
    decorate it thusly:

    >>>@with_backoff
    >>>def some_operation_that_might_fail():
    >>>    import random
    >>>    assert random.randint(0, 10) > 5

    >>>@with_backoff(exceptions=AssertionError, retries=10)
    >>>def some_otheroperation_that_might_fail():
    >>>    import random
    >>>    assert random.randint(0, 10) > 5
    """

    wrapper_kwargs = {
        EXCEPTIONS_ARG: exceptions,
        INITIAL_SLEEP_ARG: initial_sleep,
        TOTAL_RETRIES_ARG: retries,
        SLEEP_FUNCTION_ARG: sleep_function,
        CLEANUP_FUNCTION_ARG: cleanup_function,
        IS_RETRY_OK_FUNCTION_ARG: is_retry_ok_function,
    }

    def decorated(f):
        @functools.wraps(f)
        def inner(*args, **kwargs):
            kwargs.update(dict(wrapper_kwargs))
            return _backoff(f, *args, **kwargs)

        return inner

    if callable(func):
        return decorated(func)

    if func is not None:
        raise TypeError(
            "First positional argument, if provided, must be callable.")

    return decorated


def _backoff(f, *args, **kwargs):
    exceptions = kwargs.pop(EXCEPTIONS_ARG, DEFAULT_EXCEPTIONS)
    initial_sleep = kwargs.pop(INITIAL_SLEEP_ARG, DEFAULT_INITIAL_SLEEP)
    total_retries = kwargs.pop(TOTAL_RETRIES_ARG)
    sleep_function = kwargs.pop(SLEEP_FUNCTION_ARG, DEFAULT_SLEEP_FUNCTION)
    cleanup_function = kwargs.pop(
        CLEANUP_FUNCTION_ARG,
        DEFAULT_CLEANUP_FUNCTION)
    is_retry_ok_function = kwargs.pop(
        IS_RETRY_OK_FUNCTION_ARG,
        DEFAULT_IS_RETRY_OK_FUNCTION)

    sleep_time = initial_sleep
    for retry in range(total_retries):
        try:
            return f(*args, **kwargs)
        except exceptions as error:
            if not is_retry_ok_function(error):
                raise error
            time.sleep(sleep_time)
            sleep_time = sleep_function(sleep_time, retry)
            if cleanup_function is not None:
                cleanup_function()

    return f(*args, **kwargs)