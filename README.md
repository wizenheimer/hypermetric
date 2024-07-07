# Hypermetric

Hypermetric is a distributed evaluation runtime for ai native products, built on Ray's actor model and leveraging Apache Arrow for efficient data processing.

## Overview

Hypermetric utilizes Ray's distributed actor model to parallelize ML evaluations across multiple nodes:

- **Distributed Computation**: Based on Ray's actor system for fault-tolerant, scalable distributed computing
- **Data Handling**: Uses Apache Arrow for zero-copy reads and efficient inter-process communication
- **Load Balancing**: With Ray's dynamic work stealing algorithm for optimal resource utilization
- **Fault Tolerance**: Automatic task resubmission and stateful recovery for long-running evaluations with Ray
- **Abstractions**: Abstracts away complexity of distributed systems from ML workflows
- **Prototype to Production**: Enables seamless scaling from local development to large clusters


## Components

1. **Distributed Scheduler**: Optimizes task distribution based on node resources and network topology
2. **Evaluation Runner**: Executes user defined evaluation metrics in isolated environments
3. **Data Sharding**: Partitions datasets for balanced node utilization
4. **Result Aggregator**: Combines partial results using configurable reduction operations


## API

Hypermetric exposes two main decorators:

### 1. Runtime Decorator

```python
from cyyrus import runtime

# specify the runtime parameters
@runtime(
    workers = 10,
    max_runs = math.inf,
    max_retries = 3,
    delay_seconds = 5,
    exception_to_retry = Exception,
)
def main(query: str):
    # your existing code remains unaltered
    pass

# just pass the Dataset field instead
main(query = Dataset.query) # main(query = "hey, google")

```

- Automatically shards input data
- Spawns distributed workers for parallel execution
- Handles inter-node communication and result aggregation

### 2. Component Decorator

```python
from cyyrus import component
from my_library import metric_1, metric_2 # come up with our own metrics

@component(
    evals=[
          metric_1().use(
                          input = component.input("input_data"), # specify what all fields need to be captured
                          ground_truth = dataset.ground_truth, # even supports datasets
          ),
          metric_2().use(
                          input = component.input("input_data"),
                          output = component.output()),
          ]
)
def preprocess(input_data):
    # your code remains unchanged - unaltered
    return processed_data
```

- Decompose monolithic ML pipelines into testable units
- Minimal changes to existing code
- Uses stateful pools to distribute eval runs

## Contributing

Feel free to contribute to Hypermetric by sending us your suggestions, bug
reports, or cat videos. Contributions are what make the open source community
such an amazing place to be learn, inspire, and create. Any contributions you
make are **greatly appreciated**.

## License

Distributed under the MIT License. See [LICENSE](LICENSE) for more information.
Hypermetric is provided "as is" and comes with absolutely no guarantees. We take
no responsibility for existential crises induced by unpredictable results. 
If it breaks, well, that's your problem now. hehe 💀

## Credits

Hypermetric was brought to you by a hyper-caffienated developer who got bored 
at work building crappy products and should've been working on something more 
productive instead. But hey here I am.

