# Abap parallel thread utils

Initially based on this: https://blogs.sap.com/2019/03/19/parallel-processing-made-easy/

Started as an attempt to familiarize with abap parallel processing. Ended with a couple of abstract utils worth sharing: a framework to run abap threads with classes, not with function modules. Also proposes an abstraction for reducer queue, allowing to run multiple worker in threads but wait for just one task to end.

## Example

For the full example see [zthread_runner_test.prog.abap](src/zthread_runner_test.prog.abap). It implements various possibilities to use the library: 
- single task, started in parallel: directly with FM or with the wrapping class (preferable)
- multiple with queue handling in the main tasks
- multiple with queue handling in a parallel task (reducer)

Further development might go to the direction of complete **map-reduce** framework (which is already possible in fact, just lacking some `map` abstractions).

## Components

- `zcl_thread_queue_dispatcher` - mostly the code from [here](https://blogs.sap.com/2019/03/19/parallel-processing-made-easy/) with some minor modification. An util to initialize the parallel processing and queueing of workers within a limited number of threads.
- `zcl_thread_runner_slug` - abstraction for a task worker. Works together with FM `ZTHREAD_RUNNER_TEST`. See example in `run_1_parallel` method in [zthread_runner_test.prog.abap](src/zthread_runner_test.prog.abap). A subclass must implement:
    - a) `get_state_ref` to return a reference to the worker state. The state is serialized before starting parallel thread, passed serialized inside the thread and **rehydrate a new instance inside the thread**. This is due to the fact that objects cannot travel between threads in abap, just char-like data. So be aware to represent the complete state, that is needed to run the task, in a single structure! In case you want to serialize the state specifically, redefine `serialize/deserialize_state` methods
    - b) `run` - code to do the work
- `zcl_thread_reducer` - abstraction for a worker, that starts other workers in other threads. It inherits from `zcl_thread_runner_slug` and supposed to be started in parallel itself. So the idea is to compile some table-like-payload (multiple data portions to work on), put it into the reducer, and send the reducer to a thread itself, then wait for **a one single task** to complete in the main code, which will contain all the results. See example in `run_with_reducer` method in [zthread_runner_test.prog.abap](src/zthread_runner_test.prog.abap). Implement the following abstract methods:
    - a) `get_state_ref` - to return a reference to **the payload table**
    - b) `create_runner` - code to do create a worker instance - a class inherited from `zcl_thread_runner_slug`
    - c) `extract_result` - code to extract result from a worker instance and put into the payload table (which is the final result of the reducer itself)

**Important**: the sub classes of the `zcl_thread_runner_slug` and `zcl_thread_reducer` must not have constructor with parameters as they are re-instantiated and re-hydrated inside a thread. Use factory pattern or at least `create` class method to create instances.

## Technical notes

The library supposes inheriting and dynamic calls, not reusing of interfaces. This is because of potential usage as fully local classes.

## Credits

- Matthew Billingham, https://blogs.sap.com/2019/03/19/parallel-processing-made-easy/
