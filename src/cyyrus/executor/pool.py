import functools
import time
from typing import Callable, Iterable
from ray.util.multiprocessing import Pool as StatelessPool
from ray.util.multiprocessing.pool import AsyncResult


class ExecutionPool:

    def __init__(self):
        # Prevent direct instantiation of this class
        raise RuntimeError()

    @classmethod
    def create(
        cls,
        workers: int = 1,
    ):
        """
        The function creates an instance of a class with a specified number of worker processes in a
        pool.

        :param cls: The `cls` parameter in the `create` method is a reference to the class itself. It is
        used to create a new instance of the class and set up the necessary attributes for that instance
        :param workers: The `workers` parameter specifies the number of worker processes to be created
        in the pool for parallel processing. It determines how many tasks can be executed concurrently,
        defaults to 1
        :type workers: int (optional)
        :return: An instance of the class with the specified number of workers and an associated pool
        and results list is being returned.
        """
        pool = StatelessPool(processes=workers)
        instance = cls.__new__(cls)
        instance.pool = pool
        instance.results = []
        return instance

    def submit(
        self,
        func: Callable,
        iterator: Iterable,
        chunk_size: int = 1,
        max_attempts: int = 3,
        delay_seconds: int = 0,
        exception_to_retry: Exception = Exception,
        error_callback: Callable = lambda *args, **kwargs: None,
        success_callback: Callable = lambda *args, **kwargs: None,
    ):
        """
        This function submits tasks to be processed with customizable parameters such as chunk size,
        maximum attempts, delay between attempts, and callbacks for handling errors and successes.

        :param func: The `func` parameter is expected to be a callable object, such as a function or a
        method, that you want to execute on each item in the `iterator`
        :type func: Callable
        :param iterator: The `iterator` parameter in the `submit` function is expected to be an iterable
        object. This means it should be a collection of items that can be iterated over, such as a list,
        tuple, dictionary, or any other object that supports iteration. When the function is called, it
        will
        :type iterator: Iterable
        :param chunk_size: The `chunk_size` parameter in the `submit` function specifies the number of
        elements to process in each iteration from the `iterator`. It controls how many items are
        processed concurrently or in a single batch, defaults to 1
        :type chunk_size: int (optional)
        :param max_attempts: The `max_attempts` parameter specifies the maximum number of attempts to
        execute the function `func` on each chunk of the iterator `iterator`. If an attempt fails due to
        the specified `exception_to_retry`, the function will be retried up to `max_attempts` times
        before giving up, defaults to 3
        :type max_attempts: int (optional)
        :param delay_seconds: The `delay_seconds` parameter specifies the number of seconds to wait
        before retrying the function in case of an exception. This delay can help prevent overwhelming
        the system with repeated attempts in quick succession, defaults to 0
        :type delay_seconds: int (optional)
        :param exception_to_retry: The `exception_to_retry` parameter in the `submit` function specifies
        the type of exception that should trigger a retry of the function `func` when encountered during
        execution. If this exception is raised within the function `func`, the retry mechanism will be
        activated based on the specified `max_attempts` and
        :type exception_to_retry: Exception
        :param error_callback: The `error_callback` parameter in the `submit` function is a callback
        function that will be called when an error occurs during the execution of the `func`. It is a
        function that can handle or process the error in a custom way specified by the user. The default
        behavior is set to a lambda
        :type error_callback: Callable
        :param success_callback: The `success_callback` parameter in the `submit` function is a callable
        that will be executed when the function `func` is successfully executed without raising any
        exceptions. You can provide a custom function to be called when the main function completes
        successfully
        :type success_callback: Callable
        """

        def retry(max_attempts=3, delay_seconds=0, exception_to_retry=Exception):
            """
            A decorator for retrying a function call with a specified maximum number of attempts and delay between attempts.

            Args:
            - max_attempts (int): Maximum number of attempts for the function call.
            - delay_seconds (int): Time to wait between attempts.
            - exception_to_check (Exception): The exception to check. Defaults to Exception, which catches all exceptions.
            """

            def decorator(f):

                @functools.wraps(f)
                def wrapped(*args, **kwargs):
                    attempts = 0
                    while attempts < max_attempts:
                        try:
                            return f(*args, **kwargs)
                        except exception_to_retry:
                            time.sleep(delay_seconds)
                            attempts += 1
                    raise ValueError("Maximum retries exceeded")

                return wrapped

            return decorator

        func_retry_enabled = retry(
            max_attempts=max_attempts,
            delay_seconds=delay_seconds,
            exception_to_retry=exception_to_retry,
        )(func)

        res = self.pool.starmap_async(
            func=func_retry_enabled,
            iterable=iterator,
            chunk_size=chunk_size,
            callback=success_callback,
            error_callback=error_callback,
        )

        return res

    def retrieve(
        self,
        res: AsyncResult,
    ):
        """
        The `retrieve` function takes an `AsyncResult` object and returns the result of the asynchronous
        operation it represents.

        :param res: The `res` parameter in the `retrieve` function is an `AsyncResult` object
        :type res: AsyncResult
        :return: the result of the asynchronous operation represented by the `AsyncResult` object `res`
        by calling the `get()` method on it.
        """
        return res.get()
