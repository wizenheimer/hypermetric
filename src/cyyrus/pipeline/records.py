from dataclasses import asdict, astuple, dataclass, field
from datetime import datetime
from typing import Any, Dict


@dataclass
class Record:
    Component: str
    Metric: str
    Result: Any
    Inputs: Dict[str, Any] = field(default_factory=dict)
    Timestamp: datetime = field(default_factory=datetime.now)

    def __post_init__(self):
        """
        The function `__post_init__` performs type checking on attributes `Component`, `Metric`, and
        `Inputs` of an object.
        """
        if not isinstance(self.Component, str):
            raise TypeError(f"Component must be a string, got {type(self.Component).__name__}")
        if not isinstance(self.Metric, str):
            raise TypeError(f"Metric must be a string, got {type(self.Metric).__name__}")
        if not isinstance(self.Inputs, dict):
            raise TypeError(f"Input must be a dictionary, got {type(self.Inputs).__name__}")
        for key, value in self.Inputs.items():
            if not isinstance(value, list):
                raise TypeError(f"Value for key '{key}' must be a list, got {type(value).__name__}")

    def to_dict(self):
        """
        The `to_dict` function returns a dictionary representation of an object using the `asdict`
        function.
        :return: The `to_dict` method is returning a dictionary representation of the record using the
        `asdict` function.
        """
        return asdict(self)

    def to_tuple(self):
        """
        The function `to_tuple` converts an object to a tuple using the `astuple` function.
        :return: A tuple representation of the record is being returned.
        """
        return astuple(self)

    def __repr__(self):
        """
        The function `__repr__` returns a string representation of a Record object with its attributes.
        :return: The `__repr__` method is returning a formatted string that includes the values of the
        `Component`, `Metric`, `Result`, `Inputs`, and `Timestamp` attributes of the object.
        """
        return (
            f"Record(Component={self.Component}, Metric={self.Metric}, Result={self.Result}, "
            f"Input={self.Inputs}, Timestamp={self.Timestamp})"
        )
