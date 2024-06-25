from typing import Optional
import ray
from datasets import Dataset as HFDataset


class Resolver:
    def __init__(
        self,
        dataset: Optional[HFDataset] = None,  # warsaw: take in a dict of datasets
    ) -> None:
        self.records = DataStore.remote(dataset=dataset)  # type: ignore

    def resolve(
        self,
        task_id,  # warsaw: take in a task_id and dataset_id
    ):
        return ray.get(self.records.resolve.remote(task_id))


@ray.remote
class DataStore:
    def __init__(
        self,
        dataset: Optional[HFDataset] = None,  # warsaw: experiment with iterable dataset
    ):
        self.dataset = dataset
        self.records = {}
        self.count = 0

    def resolve(
        self,
        task_id,
    ):
        if task_id not in self.records.keys():
            self.records[task_id] = self.dataset[self.count] if self.dataset else None  # type: ignore
            self.count += 1

        return self.records[task_id]
