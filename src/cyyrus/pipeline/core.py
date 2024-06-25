from typing import Any, Dict, Optional
from datetime import datetime
from datasets import Dataset as HFDataset
import ray
from typeguard import typechecked

from cyyrus.metrics.base import Metric
from cyyrus.pipeline.collector import Collector
from cyyrus.pipeline.datastore import Resolver
from cyyrus.pipeline.records import Record


@ray.remote
@typechecked
class Pipeline:
    def __init__(
        self,
        dataset: Optional[HFDataset] = None,
        name: Optional[str] = None,
        workers: int = 4,
        row_limit: int = 1000,
    ):
        self.name = name if name is not None else datetime.now().strftime("%Y%m%d_%H%M%S")
        self.collector = Collector(
            pipeline_name=self.name,
            pool_size=workers,
            row_limit=row_limit,
        )
        self.resolver = Resolver(
            dataset=dataset,
        )

    def add_record(
        self,
        record: Record,
    ):
        self.collector.submit(
            record=record,
        )

    def fetch_record(  # perf: add caching
        self,
        task_id,  # warsaw: task dataset_key and task_id
    ):
        record = self.resolver.resolve(
            task_id=task_id,
        )

        return record

    def dispatch(
        self,
        metric: Metric,
        task_id: str,
        component_name: str,
        context: Dict[str, Any],
    ):
        context["dataset"] = self.fetch_record(
            task_id=task_id,
        )

        resolved_params = {}
        for key, value in metric.params.items():
            try:
                # Check if the value is a callable and call it with context
                if callable(value):
                    resolved_value = value(context)
                else:
                    resolved_value = value
                    # Use the value as is if it's not callable or a generator
            except Exception:
                resolved_value = None

            # Store the resolved value in the parameters dictionary
            resolved_params[key] = resolved_value

        metric.params = resolved_params
        metric.component_name = component_name

        record = metric.evaluate(
            task_id=task_id,
        )

        # record_ref = ray.put(
        #     record,
        # )

        self.collector.submit(
            record=record,
        )

    def save(
        self,
        reuse_pool: bool = True,
        clean_dir: bool = True,
    ):
        results = self.collector.dump(
            reuse=reuse_pool,
            clean_dir=clean_dir,
        )

        return results
