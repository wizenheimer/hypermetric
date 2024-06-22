from typeguard import typechecked
from cyyrus.constants.messages import Messages
from typing import List, Optional, Union, Callable, Dict

from pandas import DataFrame

from datasets import (
    DatasetDict,
    load_dataset,
    DatasetInfo,
    Dataset as HFDataset,
    IterableDataset,
    IterableDatasetDict,
    NamedSplit,
    Split,
)
from datasets.features import Features


@typechecked
class Dataset:
    def __init__(self):
        # Prevent direct instantiation of this class
        raise RuntimeError(Messages.dataset_initialization_failed("from_dataset or others"))

    @classmethod
    def from_dataset(
        cls,
        dataset: Optional[
            Union[
                DatasetDict,
                HFDataset,
                IterableDatasetDict,
                IterableDataset,
                Dict[str, IterableDataset],
            ]
        ] = HFDataset.from_list([]),
    ):
        """
        Create a Dataset instance from a given dataset object.

        This method handles both dictionary-like datasets (e.g., DatasetDict) which contain multiple possible datasets (like 'train', 'test') and individual or iterable datasets. It initializes the class instance with the chosen dataset and ensures that the instance has direct access to the dataset's contents as attributes.

        :param dataset: The dataset object to wrap. This can be a structured collection of datasets or a single dataset.
        :return: An initialized instance of the class Dataset that contains and manages the dataset.

        Note:
        - The method prioritizes the 'train' dataset if available when handling dictionary-like dataset types. For iterable dataset types, ensure that the initialization respects their iterable nature.
        """
        if isinstance(dataset, (DatasetDict, dict)) and "train" in dataset:
            dataset = dataset["train"]
        elif isinstance(dataset, (DatasetDict, dict)):
            dataset = next(iter(dataset.values()))

        instance = cls.__new__(cls)
        instance._dataset = dataset
        instance._add_attrs(instance.fields)
        return instance

    @classmethod
    def from_json(
        cls,
        data_files=None,
        split: Optional[Split] = None,
        **kwargs,
    ):
        """
        Create a Dataset instance from JSON files.
        :param data_files: File paths or URLs to JSON files.
        :param kwargs: Additional keyword arguments passed to the loader.
        :return: An instance of Dataset.
        """
        assert data_files is not None, Messages.missing_data_file
        dataset = load_dataset("json", data_files=data_files, split=split, **kwargs)
        return cls.from_dataset(dataset)

    @classmethod
    def from_csv(
        cls,
        data_files=None,
        split: Optional[Split] = None,
        **kwargs,
    ):
        """
        Create a Dataset instance from CSV files.
        :param data_files: File paths or URLs to CSV files.
        :param kwargs: Additional keyword arguments passed to the loader.
        :return: An instance of Dataset.
        """
        assert data_files is not None, Messages.missing_data_file
        dataset = load_dataset("csv", data_files=data_files, split=split, **kwargs)
        return cls.from_dataset(dataset)

    @classmethod
    def from_parquet(
        cls,
        data_files=None,
        split: Optional[Split] = None,
        **kwargs,
    ):
        """
        Create a Dataset instance from Parquet files.
        :param data_files: File paths or URLs to Parquet files.
        :param kwargs: Additional keyword arguments passed to the loader.
        :return: An instance of Dataset.
        """
        assert data_files is not None, Messages.missing_data_file
        dataset = load_dataset("parquet", data_files=data_files, split=split, **kwargs)
        return cls.from_dataset(dataset)

    @classmethod
    def from_arrow(
        cls,
        data_files=None,
        split: Optional[Split] = None,
        **kwargs,
    ):
        """
        Create a Dataset instance from Arrow files.
        :param data_files: File paths or URLs to Apache Arrow files.
        :param kwargs: Additional keyword arguments passed to the loader.
        :return: An instance of Dataset.
        """
        assert data_files is not None, Messages.missing_data_file
        dataset = load_dataset("arrow", data_files=data_files, split=split, **kwargs)
        return cls.from_dataset(dataset)

    @classmethod
    def from_dataframe(
        cls,
        df: DataFrame,
        features: Optional[Features] = None,
        info: Optional[DatasetInfo] = None,
        split: Optional[NamedSplit] = None,
        preserve_index: Optional[bool] = None,
    ):
        """
        Create a Dataset instance from a pandas DataFrame.
        :param df: DataFrame to convert.
        :param features: Dataset features.
        :param info: Dataset information.
        :param split: Dataset split name.
        :param preserve_index: Whether to preserve the DataFrame's index.
        :return: An instance of Dataset.
        """
        dataset = HFDataset.from_pandas(
            df=df,
            features=features,
            info=info,
            split=split,
            preserve_index=preserve_index,
        )
        return cls.from_dataset(dataset)

    @classmethod
    def from_list(
        cls,
        mapping: List[dict],
        info: Optional[DatasetInfo] = None,
        split: Optional[NamedSplit] = None,
    ):
        """
        Create a Dataset instance from a list of dictionary.
        :param mapping: List of dictionary containing the data.
        :param features: Dataset features.
        :param info: Dataset information.
        :param split: Dataset split name.
        :return: An instance of Dataset.
        """
        dataset = HFDataset.from_list(mapping=mapping, info=info, split=split)
        return cls.from_dataset(dataset)

    @classmethod
    def from_dict(
        cls,
        mapping: dict,
        features: Optional[Features] = None,
        info: Optional[DatasetInfo] = None,
        split: Optional[NamedSplit] = None,
    ):
        """
        Create a Dataset instance from a dictionary.
        :param mapping: Dictionary containing the data.
        :param features: Dataset features.
        :param info: Dataset information.
        :param split: Dataset split name.
        :return: An instance of Dataset.
        """
        dataset = HFDataset.from_dict(
            mapping=mapping,
            features=features,
            info=info,
            split=split,
        )
        return cls.from_dataset(dataset)

    def list_attributes(self):
        """
        Collects the list of available attributes
        """
        available_attrs = set(dir(self))
        if hasattr(self, "_dataset"):
            available_attrs.update(dir(self._dataset))

        return available_attrs

    @classmethod
    def from_generator(
        cls,
        generator: Callable,
        features: Optional[Features] = None,
        cache_dir: Optional[str] = None,
        keep_in_memory: bool = False,
        gen_kwargs: Optional[dict] = None,
        num_proc: Optional[int] = None,
        **kwargs,
    ):
        """
        Create a Dataset instance from a generator function.
        :param generator: A generator function yielding data.
        :param features: Dataset features.
        :param cache_dir: Directory to cache data.
        :param keep_in_memory: Whether to load the dataset into memory.
        :param gen_kwargs: Keyword arguments for the generator.
        :param num_proc: Number of processes to use.
        :param kwargs: Additional keyword arguments passed to the loader.
        :return: An instance of Dataset.
        """
        dataset = HFDataset.from_generator(
            generator=generator,
            features=features,
            cache_dir=cache_dir,  # type: ignore
            keep_in_memory=keep_in_memory,
            gen_kwargs=gen_kwargs,
            num_proc=num_proc,
        )
        return cls.from_dataset(dataset)

    def __getattr__(
        self,
        name: str,
    ):
        """
        Dynamically retrieve an attribute from the class or its wrapped dataset.
        If the attribute is not found in the class's own dictionary, this method attempts to retrieve it from the wrapped dataset object. If the attribute still isn't found, it raises a custom AttributeError.

        :param name: The name of the attribute to retrieve.
        :raises AttributeError: If the attribute is not found within the class or the wrapped dataset.
        :return: The value of the requested attribute.
        """
        if name in self.__dict__:
            return self.__dict__[name]

        try:
            return getattr(self._dataset, name)
        except AttributeError:
            raise AttributeError(
                Messages.attribute_error(
                    attribute_name=name, available_attributes=self.list_attributes()
                )
            )

    def flatten(
        self,
    ):
        """
        Flattens nested column structures in the dataset and updates class attributes accordingly.

        This method renames columns to remove nesting indicated by dots and makes them accessible as attributes on the instance. It supports attribute access to dataset columns by name, handling both renaming and attribute creation.

        Note:
        - This method may alter the dataset and attribute structure significantly.
        """
        # remove previous attrs
        self._remove_attrs(self.fields)

        # flatten the dataset
        self._dataset = self._dataset.flatten()

        # normalize the column names
        new_column_names = {col: col.replace(".", "_").lower() for col in self.fields if "." in col}
        for old_name, new_name in new_column_names.items():
            self._dataset = self._dataset.rename_column(old_name, new_name)

        # add new attra
        self._add_attrs(self.fields)

    def _add_attrs(
        self,
        attrs: List,
    ):
        """
        Adds new attributes to the class based on column names from the dataset.
        :param attrs: A list of column names to be added as attributes
        :type attrs: list
        :raises ValueError: If the input is not a list of strings
        """
        if not isinstance(attrs, List):
            raise ValueError(
                Messages.value_error(expected_type=type(List), received_type=type(attrs))
            )
        for attr in attrs:
            setattr(self, attr, (x for x in self._dataset[attr]))

    def _remove_attrs(
        self,
        attrs: List,
    ):
        """
        Removes attributes from the class based on a list of column names.
        :param attrs: A list of column names to be removed as attributes
        :type attrs: list
        :raises ValueError: If the input is not a list of strings
        """
        if not isinstance(attrs, List):
            raise ValueError(
                Messages.value_error(expected_type=type(List), received_type=type(attrs))
            )
        for attr in attrs:
            if hasattr(self, attr):
                delattr(self, attr)

    def renew_field(
        self,
        field: str,
    ):
        """
        Resets the generator for a specified field in the dataset.
        :param field: field name
        :type field: string

        Note:
        - This operation will reset the generator to allow re-iteration over the dataset values for the specified field.
        """
        if field in self.fields and hasattr(self, field):
            setattr(self, field, (x for x in self._dataset[field]))

    def get_field_value(
        self,
        field: str,
        supress_iterstop: bool = True,
    ):
        """
        Retrieves the next value for a specified field from an internal dataset generator.
        This function attempts to return the next value from a dataset generator associated with the specified field. If the generator is exhausted, the function can either return `None` or raise an exception, based on the `suppress_iterstop` parameter.

        Parameters:
        - field (str): The name of the field for which to retrieve the next value. This field must be one of the initialized fields within the dataset.
        - suppress_iterstop (bool, optional): Flag to control behavior when the generator for the specified field is exhausted. Defaults to True, which suppresses the StopIteration exception and returns `None`. If False, the function will raise an Exception indicating that no more data is available.

        Raises:
        - AttributeError: If the specified field is not a recognized dataset field.
        - Exception: If the generator is exhausted and `suppress_iterstop` is False, it raises an exception with a message indicating that no more data is available. Additionally, any unexpected errors during the retrieval will raise an exception with a relevant error message.

        Returns:
        - The next value of the specified field from the dataset, or `None` if the generator is exhausted and `suppress_iterstress` is True.
        """
        if field not in self.fields:
            raise AttributeError(
                Messages.attribute_error(
                    attribute_name=field,
                    available_attributes=self.fields,
                )
            )
        try:
            field_generator = getattr(self, field)
            return next(field_generator)
        except StopIteration:
            if supress_iterstop:
                return None
            else:
                raise Exception(Messages.generator_exhausted())

    @property
    def fields(self):
        """
        Returns the list of column names within the dataset
        :return: column names
        :rtype: list
        """
        return self._dataset.column_names

    def sample(
        self,
        max_rows: int,
        seed_value: int = 42,
    ):
        """
        Bootstrap a sample of data from the dataset
        :param seed_value: Seed for shuffle
        :type seed_value: int
        :param max_rows: Maximum count of rows
        :type max_rows: int
        :return: Sampled Dataset
        :rtype: Dataset
        """
        max_rows = min(max_rows, len(self._dataset))
        shuffled_dataset = self._dataset.shuffle(seed=seed_value).select(range(max_rows))
        return type(self).from_dataset(shuffled_dataset)

    def add_custom_field(
        self,
        name: str,
        column: str,
        func: Callable,
    ):
        """
        Adds a new field to the dataset by applying a callable to an existing column.
        :param name: The name for the new field.
        :type name: str
        :param column: The name of the column the callable will operate on.
        :type column: str
        :param func: A function to apply to the dataset column to create a new field.
        :type func: callable
        :raises ValueError: incase there's a type mismatch
        :raises AttributeError: incase the column name isn't found in field

        Note:
        - This operation will apply the function to each element of the specified column and add the resulting data as a new field to the dataset.
        """
        if not callable(func):
            raise ValueError(
                Messages.value_error(expected_type=type(callable), received_type=type(func))
            )
        if column not in self.fields:
            raise AttributeError(
                Messages.attribute_error(attribute_name=column, available_attributes=self.fields)
            )
        self._dataset = self._dataset.map(lambda ds: func(ds[column]), batched=False)
        setattr(self, name, self._dataset[name])
