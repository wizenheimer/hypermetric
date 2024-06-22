from abc import ABC, abstractmethod
from typing import Any, Dict, Generator

from typeguard import typechecked

from cyyrus.pipeline.records import Record


@typechecked
class Metric(ABC):
    def __init__(
        self,
    ):
        """
        Initialize the Metric with an empty parameters dictionary.
        """
        self.params: Dict[str, Any] = {}

    def use(
        self,
        **kwargs,
    ) -> "Metric":
        """
        Set parameters for the Metric and returns the modified Metric object.
        :return: The instance of the Metric with updated parameters.
        """
        self.params.update(kwargs)
        return self

    def resolve(
        self,
        component_name: str,
        context: Dict,
    ) -> "Metric":
        """
        Resolve parameters based on the provided context.
        :args: The context to use for resolving the parameters.
        :return: The instance of the Metric with resolved parameters.
        """
        self.component_name = component_name

        resolved_params = {}
        for key, value in self.params.items():
            try:
                # Check if the value is a callable and call it with context
                if callable(value):
                    resolved_value = value(context)
                    # Check if the value is a generator and pull one value from it
                elif isinstance(value, Generator):
                    try:
                        resolved_value = next(value)
                    except StopIteration:
                        resolved_value = None
                        # Handle case where the generator is empty
                else:
                    resolved_value = value
                    # Use the value as is if it's not callable or a generator
            except Exception:
                resolved_value = None

            # Store the resolved value in the parameters dictionary
            resolved_params[key] = resolved_value

        # Override existing params
        self.params = resolved_params
        return self

    def get_param(
        self,
        field_name: str,
    ) -> Any:
        """
        The `get_param` function retrieves a parameter value based on the provided field name from a
        dictionary of parameters.

        :param field_name: The `get_param` function takes in a `field_name` parameter, which is a string
        representing the name of the field you want to retrieve from the `params` dictionary. The
        function will return the value associated with the `field_name` key in the `params` dictionary,
        or `None
        :type field_name: str
        :return: The `get_param` method is returning the value associated with the `field_name` key in
        the `params` dictionary of the object (`self`). If the key `field_name` is not found in the
        dictionary, it will return `None`.
        """
        return self.params.get(field_name, None)

    @abstractmethod
    def evaluate(
        self,
    ) -> Record:
        """
        Abstract method to evaluate the metric. Must be implemented by subclasses.
        :returns: The record of the evaluation.
        """
        pass  # should be implemented by all the child metrics

    def export_metric(
        self,
        result: Any,
    ) -> Record:
        """
        The `record_builder` function creates a `Record` object with specified component name, metric
        name, result, and inputs.

        :param result: The `result` parameter in the `record_builder` function is used to store the
        result of a computation or operation. It is a variable of type `Any`, which means it can hold
        any type of value (e.g., integer, string, list, dictionary, etc.). This result will be
        :type result: Any
        :return: An instance of the Record class with the specified attributes: Component, Metric,
        Result, and Inputs.
        """
        return Record(
            Component=self.component_name,
            Metric=self.__class__.__name__,
            Result=result,
            Inputs=self.params,
        )

    def info(
        self,
    ) -> str:
        """
        Provides information about the Metric.
        :return: A string containing the name and description of the Metric.
        """
        return f"Name: {self.__class__.__name__}\nDescription: {self.__class__.__doc__}"
