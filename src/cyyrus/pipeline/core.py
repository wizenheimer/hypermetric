from typing import Optional
from datetime import datetime

from typeguard import typechecked

from cyyrus.metrics.base import Metric
from cyyrus.pipeline.collector import Collector
from cyyrus.pipeline.records import Record


@typechecked
class Pipeline:
    def __init__(
        self,
        name: Optional[str] = None,
        workers: int = 4,
        row_limit: int = 1000,
    ):
        """
        This Python function initializes a pipeline object with optional name, default worker count of 4, and default row limit of 1000, setting the name based on current datetime if not provided.

        :param name: The `name` parameter in the `__init__` method is a string that represents the name
        of an object. If a value is provided for `name`, it will be used as the name of the object. If
        no value is provided (i.e., `None`), the current date
        :type name: Optional[str]
        :param workers: The `workers` parameter in the `__init__` method specifies the number of worker
        threads to use for processing tasks. In this case, the default value is set to 4 if no value is
        provided when initializing an instance of the class. This parameter controls the concurrency
        level of the processing tasks, defaults to 4
        :type workers: int (optional)
        :param row_limit: The `row_limit` parameter in the `__init__` method is used to specify the
        maximum number of rows that can be processed by the `Collector` instance created within the
        class. It determines the limit for the number of rows that can be collected or processed during
        the execution of the pipeline, defaults to 1000
        :type row_limit: int (optional)
        """
        self.name = name if name is not None else datetime.now().strftime("%Y%m%d_%H%M%S")
        self.collector = Collector(
            pipeline_name=self.name,
            pool_size=workers,
            row_limit=row_limit,
        )

    def add_record(
        self,
        record: Record,
    ):
        """
        The `add_record` function submits a record to a collector.

        :param record: The `add_record` function takes a `record` parameter, which is the data that you
        want to add to the collector. This data could be any information that you want to store or
        process within the collector
        """
        self.collector.submit(record=record)

    def evaluate_and_export(
        self,
        metric: Metric,
    ):
        """
        The `evaluate` function takes a Metric object, evaluates it, and adds the result to the records.

        :param metric: The `metric` parameter in the `evaluate` method is expected to be an object of
        type `Metric`
        :type metric: Metric
        """
        self.add_record(record=metric.evaluate())

    def save(
        self,
        reuse_pool: bool = True,
        clean_dir: bool = True,
    ):
        """
        The `save` function saves the collected data in different formats based on the specified
        `format_type` parameter.

        :param format_type: The `format_type` parameter in the `save` method specifies the format in
        which the results should be saved. It can take on the following values:, defaults to dataset
        :type format_type: str (optional)
        :param path: The `path` parameter in the `save` method is used to specify the location where the
        results will be saved. If no `path` is provided, a default path will be generated using the
        current date and time along with the name of the object being saved
        :type path: Optional[str]
        :param reuse_pool: The `reuse_pool` parameter in the `save` method is a boolean flag that
        determines whether to reuse the existing data pool when saving the results. If set to `True`,
        the existing data pool will be reused. If set to `False`, a new data pool will be created when
        saving the, defaults to True
        :type reuse_pool: bool (optional)
        :param clean_dir: The `clean_dir` parameter in the `save` method determines whether to clean the
        directory before saving the results. If `clean_dir` is set to `True`, the directory will be
        cleaned before saving. If set to `False`, the directory will not be cleaned before saving,
        defaults to True
        :type clean_dir: bool (optional)
        :return: The `save` method returns the results based on the `format_type` specified. If
        `format_type` is "dataset", it returns the results as is. If `format_type` is "csv", it returns
        the results in CSV format saved at the specified path. If `format_type` is "dataframe", it
        returns the results as a pandas DataFrame. If `format_type` is
        """
        results = self.collector.dump(
            reuse=reuse_pool,
            clean_dir=clean_dir,
        )

        return results
