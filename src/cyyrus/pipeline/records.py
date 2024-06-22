from dataclasses import asdict, dataclass, field, is_dataclass
from datetime import datetime
from typing import Any, Dict, List


@dataclass
class Record:
    Component: str = "undefined"
    Metric: str = "undefined"
    Result: Any = "undefined"
    Inputs: Dict[str, Any] = field(default_factory=dict)
    Timestamp: datetime = field(default_factory=datetime.now)

    def __post_init__(
        self,
    ):
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

        def serialize_value(
            value,
        ):
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

        def flatten_and_sparsify(
            dense_dict,
            target_key="Inputs",
        ):
            """
            The function `flatten_and_sparsify` takes a dictionary, flattens it, and sparsifies a
            specific nested key before returning the modified dictionary.

            :param original_dict: The `original_dict` parameter in the `flatten_and_sparsify` function
            is a dictionary that contains key-value pairs. The function aims to flatten the dictionary
            structure and sparsify it by moving nested dictionary values to the top level with modified
            keys
            :param target_key: The `target_key` parameter in the `flatten_and_sparsify` function is used
            to specify the key in the original dictionary that contains the nested dictionary to be
            flattened and sparsified. By default, the `target_key` is set to "Inputs", but you can
            provide a different key, defaults to Inputs (optional)
            :return: The code snippet provided defines a function `flatten_and_sparsify` that takes an
            original dictionary as input, flattens it, and sparsifies it by moving nested dictionary
            values to the top level with modified keys. The function then calls `serialize_value` on an
            object `self` and passes the resulting dictionary to `flatten_and_sparsify` as
            `original_dict`.
            """

            flattened_dict = dict(dense_dict)
            sparse_dict = asdict(SubRecord())

            if target_key in dense_dict and isinstance(dense_dict[target_key], dict):
                nested_dict = flattened_dict.pop(target_key)

                for key in nested_dict:
                    if key in sparse_dict:
                        sparse_dict[key] = nested_dict[key]

                for key in sparse_dict:
                    flattened_dict[f"Input_{str(key).capitalize()}"] = sparse_dict[key]

            return flattened_dict

        dense_dict = serialize_value(asdict(self))
        return flatten_and_sparsify(dense_dict=dense_dict)

    def __repr__(
        self,
    ):
        """
        The function `__repr__` returns a string representation of a Record object with its attributes.
        :return: The `__repr__` method is returning a formatted string that includes the values of the
        `Component`, `Metric`, `Result`, `Inputs`, and `Timestamp` attributes of the object.
        """
        return (
            f"Record(Component={self.Component}, Metric={self.Metric}, Result={self.Result}, "
            f"Input={self.Inputs}, Timestamp={self.Timestamp})"
        )


@dataclass
class SubRecord:
    retrieved_context: List = field(default_factory=list)
    ground_truth_context: List = field(default_factory=list)
