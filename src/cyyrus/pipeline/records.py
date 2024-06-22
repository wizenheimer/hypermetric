from dataclasses import asdict, dataclass, field, is_dataclass
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

    def to_dict(
        self,
    ):
        """
        The `to_dict` function returns a dictionary representation of an object using the `asdict`
        function.
        :return: The `to_dict` method is returning a dictionary representation of the record using the
        `asdict` function.
        """

        def serialize_value(value):
            """
            Recursively serialize dataclass fields to JSON-compatible formats.
            Args:
                value (any): The value to serialize.
            Returns:
                any: The serialized value, or 'unserializable' if serialization fails.
            """
            if isinstance(value, (int, float, str, bool, type(None))):
                return value

            if isinstance(value, (list, tuple)):
                return [serialize_value(item) for item in value]

            if isinstance(value, dict):
                return {key: serialize_value(val) for key, val in value.items()}

            if isinstance(value, datetime):
                return value.isoformat()

            if is_dataclass(value):
                return {
                    field.name: serialize_value(getattr(value, field.name))
                    for field in value.__dataclass_fields__.values()
                }

            try:
                return str(value)
            except Exception as e:
                print(f"Error serializing {value}: {e}")
                return "unserializable"

        return serialize_value(asdict(self))

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
