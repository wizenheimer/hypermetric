import math
import random
import time
from typing import Optional, Union
from datetime import datetime
from datasets import Dataset as HFDataset
import ray
from ray.util.state import list_tasks
from typeguard import typechecked

from cyyrus.metrics.base import Metric
from cyyrus.pipeline.collector import Collector
from cyyrus.pipeline.datastore import Resolver


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

    def fetch_record(
        self,
        task_id,
    ):
        record = self.resolver.resolve(
            task_id=task_id,
        )

        return record

    def dispatch(
        self,
        metric: Metric,
    ):
        self.collector.submit(
            metric=metric,
        )

    def _exponential_backoff(
        self,
        max_retries: Union[float, int] = 10,
        max_timeout: Union[float, int] = 60,
        max_pending_percent: Union[float, int] = 10,  # flush disk
    ):
        retry_delay = 1

        start_time = time.time()
        for _ in range(int(max_retries)):
            count = 0
            total_tasks = 0
            for task in list_tasks():
                if task.name == "Worker.add_record":  # type: ignore
                    total_tasks += 1

                    if task.state in [  # type: ignore
                        "PENDING_ARGS_AVAIL",
                        "PENDING_NODE_ASSIGNMENT",
                        "PENDING_OBJ_STORE_MEM_AVAIL",
                        "PENDING_ARGS_FETCH",
                    ]:
                        count += 1

            if total_tasks == 0:
                break  # No tasks to wait for

            pending_percent = (count / total_tasks) * 100

            if pending_percent <= max_pending_percent or (time.time() - start_time) >= max_timeout:
                break

            time.sleep(retry_delay)
            retry_delay *= 2
            retry_delay += random.uniform(0, 1)

    def save(
        self,
        reuse_pool: bool = True,
        clean_dir: bool = True,
        max_retries: Union[float, int] = 10,
        max_timeout: Union[float, int] = math.inf,
        max_pending_percent: Union[float, int] = 10,
    ):
        self._exponential_backoff(
            max_retries=max_retries,
            max_timeout=max_timeout,
            max_pending_percent=max_pending_percent,
        )

        results = self.collector.dump(
            reuse=reuse_pool,
            clean_dir=clean_dir,
        )

        ray.shutdown()

        return results
