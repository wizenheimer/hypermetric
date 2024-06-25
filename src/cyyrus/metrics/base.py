from abc import ABC, abstractmethod
from typing import Any, Dict

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
        self.component_name: str = "undefined"

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
        task_id: str,
    ) -> Record:
        """
        Abstract method to evaluate the metric. Must be implemented by subclasses.
        :returns: The record of the evaluation.
        """
        return self.serialize(
            result="ok",
            task_id=task_id,
        )  # should be implemented by all the child metrics

    def serialize(
        self,
        result: Any,
        task_id: str,
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
            ID=task_id,
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
