# HyperMetric: Distributed Runtime for Evaluations
Hypermetric utilizes Ray Core to build a robust distributed runtime for running offline evaluations.

![image](https://github.com/wizenheimer/cyyrus/assets/91504165/295529fa-9d3a-4c79-8207-1fddd52f2598)

## Background and Dependencies: Understanding Ray and Its Role

### Introduction to Ray

Ray is an open-source framework that simplifies the development of distributed applications. It provides a simple, universal API for building scalable, distributed systems in Python. Ray is particularly powerful for applications involving parallel and distributed computing, making it an ideal choice for our experimental build.

### Why Ray?

1. **Scalability:** Ray can scale from a single machine to a cluster of thousands of machines, making it suitable for both small-scale and large-scale evaluations.
2. **Flexibility:** Ray supports various paradigms, including stateless and stateful computations, allowing developers to use the most appropriate model for their tasks.
3. **Ease of Use:** Ray’s API is designed to be simple and intuitive, enabling rapid development and reducing the complexity of managing distributed systems.
4. **Performance:** Ray’s architecture is optimized for high performance, ensuring efficient task distribution and execution.

### Key Components of Ray in This Build

1. **Ray Core:** The core library provides the foundational building blocks for distributed computing, including remote functions (tasks) and actors.
2. **Stateless Pools:** These pools handle tasks that do not require maintaining state between executions, allowing for efficient parallelization.
3. **Stateful Pools:** These pools manage tasks that need to maintain state, such as storing intermediate results or handling long-running processes.

## Overview of the Experimental Build

### Distributed Runtime for Evaluations

Our experimental build leverages Ray Core to create a distributed runtime specifically designed for running evaluations. The key components and their interactions are detailed below:

1. **Component Decorator (`@component`)**
    - **Function:** Modularizes user code into independent components.
    - **Role:** Converts functions into components for separate evaluation, enabling parallel execution.
2. **Runtime Decorator (`@runtime`)**
    - **Function:** Parallelizes the main driver code.
    - **Role:** Dispatches tasks to stateless pools for parallel execution, ensuring efficient use of resources.
3. **Execution Flow**
    - **Input and Output Handling:** Components collect inputs and outputs, managing dependencies internally.
    - **Evaluation Requests:** Requests for evaluations are submitted to stateful actor pools, which manage execution and storage.
4. **Stateful Actor Pool Management**
    - **Task Handling:** Executes evaluations and stores results in memory.
    - **Periodic Flushing:** Flushes results into Parquet format once thresholds are reached, preventing data loss and managing memory usage.
5. **Data Reconciliation**
    - **Pipeline Object:** Collects all flushed metrics after evaluations.
    - **Dataset Compilation:** Combines metrics into a comprehensive dataset, providing an overview of the entire run.

## Benefits of Using Ray

- **Efficient Task Distribution:** Ray’s architecture allows for seamless distribution of tasks across multiple workers, optimizing resource utilization.
- **Fault Tolerance:** Periodic flushing of results prevents data loss and minimizes the impact of worker failures.
- **Memory Management:** Maintaining state hygiene and preventing OOM errors ensures stable and reliable execution of evaluations.

## Open Issues 
Hypermetric is still a work in progress but is sufficiently functional for demonstrations and waving it around in meetings. There are several areas we could improve to streamline its performance, hence the next iteration might be a complete overhaul. Our plan is to utilize external APIs unless a simpler solution for running evaluations is identified. 
- **Backpressure**: We need to determine how to implement backpressure in the actor pool effectively.
- **Non-Blocking Schedules**: The scheduler currently operates without blocking, which might require adjustments to introduce necessary blocking mechanisms.

- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 
