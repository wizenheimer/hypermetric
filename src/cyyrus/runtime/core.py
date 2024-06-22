import inspect
import math

from collections.abc import Iterable
from typing import Callable

from cyyrus.runtime.pool import ExecutionPool


class Executor:
    def __init__(
        self,
        func: Callable,
        workers: int = 1,
        max_runs: float = math.inf,
        max_attempts: int = 3,
        delay_seconds: int = 0,
        exception_to_retry=Exception,  # type: ignore
        error_callback: Callable = lambda *args, **kwargs: None,
        success_callback: Callable = lambda *args, **kwargs: None,
    ):
        """
        This function initializes an object with parameters for executing a given function with
        specified settings such as number of workers, maximum runs, maximum attempts, delay, exception
        handling, and callbacks.

        :param func: The `func` parameter in the `__init__` method is expected to be a Callable type,
        which means it should be a function or a method that can be called. This parameter represents
        the function that will be executed by the code
        :type func: Callable
        :param workers: The `workers` parameter specifies the number of worker threads that will be used
        to execute the function concurrently in the `ExecutionPool`, defaults to 1
        :type workers: int (optional)
        :param max_runs: The `max_runs` parameter in the `__init__` method represents the maximum number
        of times the function will be executed before stopping. If the function does not succeed within
        the specified number of runs, it will stop executing. The default value for `max_runs` is set to
        positive infinity (`
        :type max_runs: float
        :param max_attempts: The `max_attempts` parameter in the `__init__` method specifies the maximum
        number of attempts allowed to execute the function `func` successfully before giving up. If the
        function encounters an error or exception that is specified in `exception_to_retry`, it will
        retry executing the function up to `max, defaults to 3
        :type max_attempts: int (optional)
        :param delay_seconds: The `delay_seconds` parameter in the `__init__` method represents the
        amount of time, in seconds, to wait before retrying the function in case of an exception. This
        delay is useful for implementing a retry mechanism with a pause between each attempt to prevent
        overwhelming the system or API being called, defaults to 0
        :type delay_seconds: int (optional)
        :param exception_to_retry: The `exception_to_retry` parameter in the `__init__` method is used
        to specify the type of exception that should trigger a retry of the function `func`. If the
        function `func` raises an exception of the specified type, the retry mechanism will be activated
        based on the other parameters like
        :type exception_to_retry: Exception
        :param error_callback: The `error_callback` parameter in the `__init__` method is a callback
        function that will be called when an error occurs during the execution of the function `func`.
        It is a function that can be customized to handle or log errors in a specific way based on the
        requirements of the program
        :type error_callback: Callable
        :param success_callback: The `success_callback` parameter in the `__init__` method is a callable
        that represents a function or method that will be called when the main function `func` executes
        successfully without raising any exceptions. It allows you to specify a custom callback function
        that will be triggered upon successful completion of the main
        :type success_callback: Callable
        """

        self.pool = ExecutionPool.create(workers=workers)
        self.func = func
        self.max_runs = max_runs
        self.max_attempts = max_attempts
        self.delay_seconds = delay_seconds
        self.exception_to_retry = exception_to_retry
        self.error_callback = error_callback
        self.success_callback = success_callback

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
            while max_runs >= 0:
                max_runs -= 1
                yield tuple(fixed_values)

        # Create an iterator that yields the combined values in the correct order
        for elements in zip(*iterators):
            max_runs -= 1

            combined = [None] * len(args)
            iter_iter = iter(elements)
            fixed_iter = iter(fixed_values)

            if max_runs < 0:
                return

            for index, type_ in positions:
                if type_ == "iter":
                    combined[index] = next(iter_iter)
                else:
                    combined[index] = next(fixed_iter)

            yield tuple(combined)

    def __call__(
        self,
        *func_args,
        **func_kwargs,
    ):
        """
        The function takes arguments, flattens them, creates an iterator, and submits a task to a pool
        for execution.
        :return: The function returns the result of the function call that is submitted to a
        pool for asynchronous execution. The result is retrieved from the pool using the `retrieve`
        method of the pool object.
        """

        self.args = self._flatten_args(self.func, *func_args, **func_kwargs)
        iterator = self._yield_args(max_runs=self.max_runs)

        res = self.pool.submit(
            func=self.func,
            iterator=iterator,
            max_attempts=self.max_attempts,
            delay_seconds=self.delay_seconds,
            exception_to_retry=self.exception_to_retry,
            error_callback=self.error_callback,
            success_callback=self.success_callback,
        )

        return self.pool.retrieve(res)


def runtime(
    workers: int = 1,
    max_runs: float = math.inf,
    max_attempts: int = 3,
    delay_seconds: int = 0,
    exception_to_retry=Exception,  # type: ignore
    error_callback: Callable = lambda *args, **kwargs: None,
    success_callback: Callable = lambda *args, **kwargs: None,
):
    """
    The `executor` function is a decorator factory that takes arguments and keyword arguments to create
    a decorator that wraps a function with an `Executor` instance.
    :return: A decorator function is being returned.
    """

    def decorator(func: Callable):
        return Executor(
            func=func,
            workers=workers,
            max_runs=max_runs,
            max_attempts=max_attempts,
            delay_seconds=delay_seconds,
            exception_to_retry=exception_to_retry,
            error_callback=error_callback,
            success_callback=success_callback,
        )

    return decorator
