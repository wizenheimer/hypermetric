from cyyrus.composer.core import Composer
from cyyrus.pipeline.core import Pipeline

import ray
from ray.util.actor_pool import ActorPool

from datasets import concatenate_datasets


class Pool:
    def __init__(self, pipeline: Pipeline, pool_size: int, row_limit: int):
        """
        Creates a pool of Workers actors.

        :param pipeline: The pipeline object defining the workflow for each worker.
        :param pool_size: The number of worker actors to create in the pool.
        :param row_limit: The maximum number of records each worker should handle before flushing to disk.
        :return: An ActorPool object containing all initialized worker actors.
        """
        pool_size = max(pool_size, 1)  # Ensure there is at least one actor in the pool
        actors = [Composer.remote(pipeline=pipeline, row_limit=row_limit) for _ in range(pool_size)]
        self.pool = ActorPool(actors)

    def submit(self, record):
        """
        Submits a record to a pool of composer workers.

        :param record: A single data record to add to the buffer.
        """
        self.pool.submit(lambda actor, value: actor.add_record.remote(value), record)

    def drain(self):
        """
        Drains all tasks from the pool of workers, ensuring that all pending operations are completed.
        """
        while self.pool.has_next():
            self.pool.get_next_unordered()

    def reset(self, reuse=True):
        """
        Resets the actor pool by first draining it and then either cleaning up or reinitializing idle actors.

        :param reuse: Boolean flag indicating whether to reinitialize idle actors (True) or permanently close them (False).
        """
        self.drain()

        idle_actors = []
        while self.pool.has_free():
            idle_actor = self.pool.pop_idle()
            ray.get(idle_actor.reset.remote())
            if reuse:
                idle_actors.append(idle_actor)

        for actor in idle_actors:
            self.pool.push(actor)

    def dump(self, reuse=True):
        """
        Prepares datasets by closing workers in the pool, compacts data, and optionally resets actors.

        :param reuse: Boolean flag indicating whether to reinitialize workers (True) or permanently close them (False) after compaction.
        :return: A combined dataset or None if no datasets were generated.
        """
        self.drain()

        datasets = []
        idle_actors = []
        while self.pool.has_free():
            idle_actor = self.pool.pop_idle()

            ray.get(idle_actor.reset.remote())

            dataset = ray.get(idle_actor.compaction.remote())
            datasets.append(dataset)

            if reuse:
                idle_actors.append(idle_actor)

        for actor in idle_actors:
            self.pool.push(actor)

        if datasets:
            combined_dataset = concatenate_datasets(dsets=datasets)  # type: ignore
            return combined_dataset

        return None
