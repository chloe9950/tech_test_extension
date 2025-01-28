import logging
from functools import wraps


def log_execution(func):
    """
    A decorator to log the execution of a function.
    """
    @wraps(func)
    def wrapper(*args, **kwargs):
        logging.info(f"Executing: {func.__name__}")
        result = func(*args, **kwargs)
        logging.info(f"Completed: {func.__name__}")
        return result
    return wrapper


def handle_exceptions(func):
    """
    A decorator to catch and log exceptions for a function.
    """
    @wraps(func)
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except Exception as e:
            logging.error(f"Exception in {func.__name__}: {e}")
            raise
    return wrapper