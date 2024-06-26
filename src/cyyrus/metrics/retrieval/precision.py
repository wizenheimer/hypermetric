from typing import List

from typeguard import typechecked

from cyyrus.metrics.base import Metric


@typechecked
class Precision(Metric):
    def use(
        self,
        retrieved_context=List[str],
        ground_truth_context=List[str],
    ):
        # Use super to pass along the named parameters explicitly
        super().use(
            retrieved_context=retrieved_context,
            ground_truth_context=ground_truth_context,
        )
        return self

    def evaluate(
        self,
    ):
        _ = self.get_param("retrieved_context")
        _ = self.get_param("ground_truth_context")

        return self.serialize(
            result="ok",
        )
