import inspect
import math

from collections.abc import Iterable
from typing import Callable

from cyyrus.executor.pool import ExecutionPool


class Executor:
    def __init__(
        self,
        func: Callable,
        *func_args,
        **func_kwargs,
    ):
        """
        The function initializes an object with a specified function, arguments, and keyword arguments.

        :param func: The `func` parameter in the `__init__` method is expected to be a callable object,
        such as a function or a method. This parameter will be stored in the `self.func` attribute of
        the class instance
        :type func: Callable
        """
        self.func = func
        self.args = self._flatten_args(func, *func_args, **func_kwargs)
        self.pool = None

    def _flatten_args(
        self,
        func: Callable,
        *func_args,
        **func_kwargs,
    ):
        """
        The function `_flatten_args` takes a function, its arguments, and keyword arguments, filters out
        unnecessary keyword arguments, binds the provided arguments to the function's signature, and
        returns purely positional arguments.

        :param func: The `func` parameter in the `_flatten_args` function is expected to be a Callable
        type, which means it should be a function or a method that can be called
        :type func: Callable
        :return: The function `_flatten_args` returns a list of purely positional arguments based on the
        provided arguments (`func_args` and `func_kwargs`) and the function signature of the `func`
        function.
        """
        # Create signature based on the function to be decorated
        sig = inspect.signature(func)
        param_names = list(sig.parameters.keys())

        # Filter out kwargs that are not in the function signature
        filtered_kwargs = {k: v for k, v in func_kwargs.items() if k in param_names}

        # Limit positional args to those not already provided as keyword args
        limited_args = []
        for arg, param_name in zip(func_args, param_names):
            if param_name not in filtered_kwargs:
                limited_args.append(arg)

        # Attempt to bind the provided arguments to the function's signature
        bound_args = sig.bind_partial(*limited_args, **filtered_kwargs)
        bound_args.apply_defaults()

        # Convert bound_args to purely positional arguments
        return [bound_args.arguments[param] for param in param_names]

    def _yield_args(
        self,
        max_runs: float = math.inf,
    ):
        """
        The function `_yield_args` generates combinations of fixed values and iterators up to a
        specified number of runs.

        :param max_runs: The `max_runs` parameter in the `_yield_args` function specifies the maximum
        number of iterations or runs that the function will perform before stopping. By default, it is
        set to `math.inf`, which means that the function will continue running until explicitly stopped
        or until it encounters a `return` statement
        :type max_runs: float
        :return: The code snippet provided is a Python generator function named `_yield_args`. It yields
        combinations of values from multiple iterators and fixed values. The function first categorizes
        the input arguments into iterators and fixed values based on their types. If there are no
        iterators, it yields tuples of fixed values until the `max_runs` limit is reached or one of the iterator exhausts
        """
        positions = []
        iterators = []
        fixed_values = []
        args = self.args

        for index, arg in enumerate(args):
            if isinstance(arg, Iterable) and not isinstance(arg, (str, bytes, list)):
                iterators.append(arg)
                positions.append((index, "iter"))
            else:
                fixed_values.append(arg)
                positions.append((index, "fixed"))

        if len(iterators) == 0:
            while max_runs > 0:
                max_runs -= 1
                yield tuple(fixed_values)

        # Create an iterator that yields the combined values in the correct order
        for elements in zip(*iterators):
            max_runs -= 1

            combined = [None] * len(args)
            iter_iter = iter(elements)
            fixed_iter = iter(fixed_values)

            if max_runs == 0:
                return

            for index, type_ in positions:
                if type_ == "iter":
                    combined[index] = next(iter_iter)
                else:
                    combined[index] = next(fixed_iter)

            yield tuple(combined)

    def __call__(
        self,
        *args,
        **kwargs,
    ):
        """
        The function raises a NotImplementedError when directly called, instructing to use the 'execute'
        method with specific parameters instead.
        """
        raise NotImplementedError(
            "Direct call to this function is not allowed. "
            "Please use the 'execute' method with appropriate parameters. "
            "Example: instance.execute(workers=4, max_runs=10, chunk_size=1)"
        )

    def execute(
        self,
        workers: int = 1,
        max_runs: int = None,
        chunk_size: int = 1,
        max_attempts: int = 3,
        delay_seconds: int = 0,
        exception_to_retry: Exception = Exception,
        error_callback: Callable = lambda *args, **kwargs: None,
        success_callback: Callable = lambda *args, **kwargs: None,
    ):
        """
        The `execute` function in Python uses multiprocessing to execute a given function with specified
        parameters and callbacks.

        :param workers: The `workers` parameter specifies the number of worker processes to use in the
        pool for executing the tasks concurrently. It determines how many tasks can be processed
        simultaneously, defaults to 1
        :type workers: int (optional)
        :param max_runs: The `max_runs` parameter in the `execute` function specifies the maximum number
        of runs or iterations that will be executed. If this parameter is set to a specific number, the
        function will only run that number of times before stopping. If it is set to `None`, the
        function will run indefinitely
        :type max_runs: int
        :param chunk_size: The `chunk_size` parameter in the `execute` function determines the number of
        tasks that will be sent to each worker process at a time. It controls how the iterable of
        arguments is divided into chunks before being processed by the worker pool, defaults to 1
        :type chunk_size: int (optional)
        :param max_attempts: The `max_attempts` parameter in the `execute` function specifies the
        maximum number of attempts to retry executing the function in case of a specified exception. If
        the function raises the specified exception, it will be retried up to `max_attempts` times
        before giving up, defaults to 3
        :type max_attempts: int (optional)
        :param delay_seconds: The `delay_seconds` parameter in the `execute` function represents the
        number of seconds to wait before retrying a failed task. If a task fails, the function will wait
        for the specified number of seconds before attempting to run the task again. This delay can help
        prevent overwhelming a system with too many, defaults to 0
        :type delay_seconds: int (optional)
        :param exception_to_retry: The `exception_to_retry` parameter in the `execute` method is used to
        specify the type of exception that should trigger a retry of the function being executed. By
        default, it is set to `Exception`, which means that any exception of type `Exception` will be
        retried according to the specified
        :type exception_to_retry: Exception
        :param error_callback: The `error_callback` parameter in the `execute` method is a callable
        function that will be executed when an error occurs during the execution of the function. It is
        a callback function that allows you to handle errors or perform specific actions when an error
        occurs. By default, it is set to a lambda
        :type error_callback: Callable
        :param success_callback: The `success_callback` parameter in the `execute` method is a callable
        that will be executed when a task is successfully completed by the worker processes in the pool.
        You can pass a function or method as the `success_callback` parameter to perform any actions you
        want to take when a task is successfully
        :type success_callback: Callable
        :return: The `execute` function returns the result of the asynchronous execution of the
        `func_retry_enabled` function using the `starmap_async` method of the `Pool` object. The result
        is obtained by calling the `get()` method on the returned result object `res`.
        """

        self.pool = ExecutionPool.create(workers=workers) if self.pool is None else self.pool

        iterator = self._yield_args(max_runs=max_runs)

        res = self.pool.submit(
            func=self.func,
            interator=iterator,
            chunk_size=chunk_size,
            max_attempts=max_attempts,
            delay_seconds=delay_seconds,
            exception_to_retry=exception_to_retry,
            error_callback=error_callback,
            success_callback=success_callback,
        )

        return self.pool.retrieve(res)


def executor(
    *func_args,
    **func_kwargs,
):
    """
    The `executor` function is a decorator factory that takes arguments and keyword arguments to create
    a decorator that wraps a function with an `Executor` instance.
    :return: A decorator function is being returned.
    """

    def decorator(func: Callable):
        return Executor(func, *func_args, **func_kwargs)

    return decorator
