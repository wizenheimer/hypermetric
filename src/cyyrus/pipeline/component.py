from functools import wraps
from inspect import signature
from typing import Callable, List, Optional

from cyyrus.metrics.base import Metric
from cyyrus.pipeline.core import Pipeline


class Component:

    def __init__(
        self,
        pipeline: Pipeline,
        name: Optional[str] = None,
        evals: Optional[List[Metric]] = None,
    ):
        """
        The function intializes the Component with a pipeline, name, and optional list of
        evaluation metrics.

        :param pipeline: The `pipeline` parameter is of type `Pipeline` and is required for initializing
        an object of this class. It seems like the `pipeline` parameter is an essential part of the
        object's state or functionality
        :type pipeline: Pipeline
        :param name: The `name` parameter in the `__init__` method is an optional parameter that allows
        you to specify a name for an instance of the class. If a name is not provided when creating an
        instance, it will default to `None`
        :type name: Optional[str]
        :param evals: The `evals` parameter in the `__init__` method is an optional parameter of type
        `List[Metric]`. If a value is provided for `evals` when creating an instance of the class, it
        will be stored in the `evals` attribute of the object. If
        :type evals: Optional[List[Metric]]
        """
        self.pipeline = pipeline
        self.name = name
        self.evals = evals if evals is not None else []

    def __call__(
        self,
        func: Callable,
    ):
        """
        The function is a decorator that wraps another function, captures its input and output as context, and evaluates specified metrics based on this context.

        :param func: The `func` parameter in the code snippet represents a function that will be wrapped
        by the decorator defined in the `__call__` method. This function will be called when the wrapper
        function is invoked. The `func` parameter is of type `Callable`, which indicates that it should
        be a callable
        :type func: Callable
        :return: The code snippet defines a decorator that wraps a given function. The decorator
        captures the arguments passed to the function, calls the function, stores the input and output
        values in a context dictionary, and evaluates metrics based on the context. The wrapper function
        is returned as the result of the decorator.
        """

        @wraps(func)
        def wrapper(*args, **kwargs):

            # Component Module
            if not self.name:
                self.name = func.__name__

            # Convert args into a dictionary of kwargs based on function signature
            sig = signature(func)

            bound_arguments = sig.bind(*args, **kwargs)
            bound_arguments.apply_defaults()  # Apply default values

            all_arguments = bound_arguments.arguments  # This gives us a dictionary of all arguments

            # Call the function
            result = func(*args, **kwargs)

            # Store Context
            context = {
                "input": all_arguments,  # kwargs,
                "output": result,
            }

            # Evaluate the metrics
            [
                self.pipeline.evaluate_and_export(
                    metric=metric.resolve(component_name=self.name, context=context)
                )
                for metric in self.evals or []
            ]

            return result

        return wrapper

    @classmethod
    def input(
        cls,
        field: Optional[str] = None,
    ):
        """
        This retrieves a specific field value from a context dictionary if provided, or the entire input dictionary if no field is specified.

        :param cls: In the given code snippet, `cls` is a reference to the class itself. It is used in a
        class method to refer to the class and its attributes or methods
        :param field: The `field` parameter in the `input` method is a string that represents a specific
        field that you want to retrieve from the input data. If `field` is provided, the method will
        return the value of that specific field from the input data. If `field` is not provided (or
        :type field: Optional[str]
        :return: The `input` method is a class method that takes an optional `field` parameter. It
        returns a lambda function that retrieves the value of the specified `field` from the `input`
        dictionary in the `context` parameter. If no `field` is specified, it returns the entire `input`
        dictionary from the `context`.
        """
        return lambda context: (
            context.get("input", {}).get(field, "")
            if field
            else (context.get("input") if context.get("input", {}) else "")
        )

    @classmethod
    def output(
        cls,
        field: Optional[str] = None,
    ):
        """
        This retrieves a specified field from a context dictionary if provided, or the entire 'output' dictionary if no field is specified.

        :param cls: In the provided code snippet, `cls` is a reference to the class itself. In this
        context, it is used as a decorator for a class method `output`. The `@classmethod` decorator
        indicates that the `output` method is a class method, which means it can be called on the
        :param field: The `field` parameter in the `output` method is a string that specifies the field
        to retrieve from the `output` dictionary in the `context` parameter. If `field` is provided, the
        method will return the value of that specific field from the `output` dictionary. If `field
        :type field: Optional[str]
        :return: The `output` method is a class method that takes an optional `field` parameter. It
        returns a lambda function that takes a `context` parameter and retrieves the value of the
        specified `field` from the "output" dictionary in the `context`. If no `field` is specified, it
        returns the entire "output" dictionary from the `context`.
        """
        return lambda context: (
            context.get("output", {}).get(field, "")
            if field
            else (context.get("output") if context.get("output", {}) else "")
        )
