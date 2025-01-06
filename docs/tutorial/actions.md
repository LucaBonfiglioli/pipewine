# ⚙️ Actions

## Overview

In this section you will learn what are the main operational abstractions that define how Pipewine transforms data, how to interact with them and how to build re-usable building blocks for your data pipelines.

!!! note

    This section assumes you already know the basics on the way the data model is structured. If you haven't already, go take a look at the [Data Model](../data) section. 

The main components that are explained in this section include:

- **Sources** - Action that creates Pipewine datasets from external storages.
- **Sinks** - Action that saves Pipewine datasets to external storages.
- **Operators** - Action that transforms Pipewine dataset/s into Pipewine dataset/s.
- **Mappers** - Applies the same operation to every sample of a dataset.
- **Grabber** - Distributes work to a multi-processing pool of workers.

## Types of Actions

Pipewine uses the broad term "Action" to denote any of the following components:

- `DatasetSource` - A component that is able to create one or more `Dataset` objects.  
- `DatasetSink` - A component that is able to consume one or more `Dataset` objects. 
- `DatasetOperator` - A components that takes as input and returns one or more `Dataset` objects.

In general, actions can receive and return one of the following:

- A single `Dataset` object.
- A `Sequence` of datasets.
- A `tuple` of datasets.
- A `Mapping` of strings to datasets.
- A `Bundle` of datasets.

Specific types of actions **statically** declare the type of inputs and outputs they accept/return to ensure that their type is always known at development time (as soon as you write the code).

!!! example

    `CatOp` is a type of `DatasetOperation` that concatenates one or more datasets into a single one, therefore, its input type is `Sequence[Dataset]` and its output is `Dataset`.

    Any modern static type checker such as `mypy` or `PyLance`  will complain if you try to pass anything else to it, preventing such errors at dev time.

!!! tip

    As a general rule of thumb of what type of input/output to choose when implementing custom actions:

    - Use `Dataset` when you want to force that the action accepts/returns exactly one dataset.
    - Use `Sequence` when you need the operation to accept/return more than one dataset, but you don't know a priory how many, their order or their type.
    - Use `tuple` in settings similar to the `Sequence` case but you also know at dev time the exact number of datasets, their order and their type. Tuples, contrary to `Sequences`, also allow you to specify the type of each individual element.
    - Use `Mapping` when you need to accept/return collections of datasets that do not have any ordering relationship, but are instead associated with an arbitrary (not known a priori) set of string keys.
    - Use `Bundle` in settings similar to the `Mapping` case but you also know at dev time the exact number of datasets, the name of their keys and their type. More details on `Bundle` in the next sub-section. 

### Bundle objects

Pipewine `Bundle[T]` are objects that have the following characteristics:

- They behave like a Python `dataclass`.
- The type of all fields is bound to `T`.
- Field names and types are statically known and cannot be modified.
- They are anemic objects: they only act as a data container and define no methods.

!!! note

    You may already have encountered the `Bundle` type without noticing when we introduced the [TypedSample](../data#typedsample). `TypedSample` is a `Bundle[Item]`.

!!! example

    Creating a `Bundle` is super easy:

    ``` py
    # Inherit from Bundle[T] and declare some fields
    class MyBundle(Bundle[str]):
        foo: str
        bar: str

    # Constructs like a dataclass
    my_bundle = MyBundle(foo="hello", bar="world")

    # Access content with dot notation
    my_bundle.foo # >>> hello
    my_bundle.bar # >>> world

    # Convert to a plain dict[str, T] when needed
    my_bundle.as_dict() # >>> {"foo": "hello", "bar": "world"}

    # Create from a dictionary
    my_bundle_2 = MyBundle.from_dict({"foo": "hello", "bar": "world"})
    ```

!!! note

    Despite their similarity `Bundle` objects **do not** rely on Pydantic and **do not** inherit from `BaseModel`. Do not expect them to provide validation, parsing or dumping functionalities, as they are essentially dataclasses plus some constraints.


## Eager/Lazy Behavior


Suppose we need to load and display N images stored as files in a directory, comparing the two approaches:

- **Eager** - Every computation is carried out as soon as the necessary data is ready.
- **Lazy** - Every operation is performed only when it cannot be delayed anymore.

Let's explore the trade-off between the two approaches:

| **Eager**                                                                                                      | **Lazy**                                                                                           |
| -------------------------------------------------------------------------------------------------------------- | -------------------------------------------------------------------------------------------------- |
| ❌ Long initialization time: all images must be loaded before the first one is displayed.                       | ✅ Short initialization time: no loading is done during initialization.                             |
| ✅ When the user requests an image, it is immediately available.                                                | ❌ When the user requests an image, it must wait for the application to load it before viewing it.  |
| ❌ High (and unbounded) memory usage, necessary to keep all images loaded at the same time.                     | ✅ Always keep at most one image loaded into memory.                                                |
| ❌ Risk of performing unnecessary computation: what if the user only wants to display a small subset of images? | ✅ If the user is not interested in a particular image, it won't be loaded at all.                  |
| ✅ Accessing an image multiple times has no additional cost.                                                    | ❌ Accessing an image multiple times has a cost proportional to the number of times it is accessed. |
| ✅ Optimizations may be easier and more effective on batched operations.                                        | ❌ Optimizations are harder to perform or less effective on small batches accessed out-of-order.    |

As you can see, there are positive and negative aspects in both approaches. You may be inclined to think that for this silly example of loading and displaying images, the lazy approach is clearly better. After all, who would use a software that needs to load the entire photo album of thousands of photos just to display a single image?

As you may have noticed in the previous section, Pipewine allows you to choose both approaches, but when there is no clear reason to prefer the eager behavior, its components are implemented so that they behave **lazily**: `Item` only reads data when requested to do so, `LazyDataset` creates the actual `Sample` object upon indexing. As you will see, most Pipewine actions follow a similar pattern.

This decision, inherited from the old Pipelime library, is motivated mainly by the following reasons:

- Data pipelines may operate on large amounts of data that we cannot afford to load upfront and keep loaded into system memory. Even small datasets with hundreds of images will impose a significant memory cost on any modern machine. Larger datasets may not be possible to load at all!
- Until a time machine is invented, there is no way to undo wasteful computation. Suppose you have an image classification dataset and you want to select all samples with the category "horse": there is no point in loading an image once you know that it's not what you are looking for. Lazy behavior prevents this kind of unnecessary computation by design.
- The risk of performing some computation twice can be mitigated by using caches with the right eviction policy.

## Parallelism

Pipewine provides you with a `Grabber` object, a simple tool allows you to iterate over a sequence with a parallel pool of multi-processing workers.

Why multi-processing and not multi-threading? Because of [GIL](https://wiki.python.org/moin/GlobalInterpreterLock) (Global Interpreter Lock), multi-threading in Python allows concurrency but not parallelism, effectively granting the same speed as a single-threaded program.

<iframe style="border:none" width="800" height="450" src="https://whimsical.com/embed/S8SpLwdz7iiZ9HZFsjjJYB@3CRerdhrAq66ytNqTd718zzy"></iframe>

To effectively parallelize our code we are left with two options:

1. Avoid using Python, and instead write components in other languages such as C/C++, then create bindings that allow the usage from Python. 
2. Bite the bullet and use multi-processing instead of multi-threading. If compared with threads, processes are a lot more expensive to create and manage, often requiring to serialize and deserialize the whole execution stack upon creation.

The second option is clearly better: 

- You still get the option to parallelize work in a relatively easy way without having to integrate complex bindings for execution environments other than the Python interpreter. 
- Process creation and communication overhead becomes negligible when dealing with large amounts of relatively independent jobs that don't need much synchronization. This is a relatively common scenario in data pipelines.
- It does not prevent you to implement optimized multi-threaded components in other languages. Think about it: whenever you use tools like Numpy inside a Pipewine action, you are doing exactly this. 

### Grabber

To parallelize tasks with Pipewine you can use a `Grabber` object: a simple wrapper around a multiprocessing `Pool` of workers iterate through an ordered sequence of objects.

!!! note

    `Grabber` objects can work on arbitrary sequences of objects, provided they are serializable.

The `Grabber` object also does a few nice things for you:

- Manages the lifecycle of the underlying pool of processes.
- Handles exceptions freeing resources and returning the errors transparently to the parent process.
- Automatically invokes a callback function whenever a task is completed. The callback is run by the individual workers.

When creating a `Grabber` you will need to choose three main parameters that govern the execution:

- `num_workers`: The number of concurrent workers. Ideally, in a magic world where all workload is perfectly distributed and communication overhead is zero, the total time is proportional to `num_jobs / num_workers`, so the higher the better. In reality:
    - Work is not perfectly distributed. If you need to run 100 jobs with 3 workers, 2 workers will be assigned 33 jobs each, but the 3rd worker is left with 34 jobs. 
    - Different jobs may have different computational costs. If you need to run 100 jobs with 4 workers, equal splits of 25 each may not guarantee that the actual workload is evenly split: a "lucky" worker may get the 25 easiest jobs and complete them way before the others have finished running. 
    - Processes are expensive to create. With small enough workloads, a single process may actually be faster than a concurrent pool.
    - Everytime an object is passed from one process to the other it needs to be serialized and then de-serialized. This can become quite expensive when dealing with large data structures.
    - Processes need synchronization mechanisms that temporarily halt the computation.
    - Sometimes the bottleneck is not the computation itself: if a single process reading data from the network completely saturates your bandwidth, adding more processes won't fix the problem.
    - Sometimes multiprocessing can be much slower than single processing. A typical example is when concurrently writing to a mechanical HDD, causing it to waste a lot of time going back and forth the writing locations.  
    - Your code may already be parallelized. Whenever you use libraries like Numpy, PyTorch, OpenCV (and many more), you are calling C/C++ bindings that run very efficient multi-threaded code outside of the Python interpreter, or even code that runs on devices other than your CPU (e.g. CUDA-enabled GPUs). Adding multiprocessing in these cases will only add overhead costs to something that already uses your computational resources nearly optimally.
    - Due to memory constraints the number of processes you can keep running without swapping (and thus severely compromising the execution speed) may be limited to a small number. E.g. if each process needs to keep loaded a 10GB deep learning model and the available memory is just 32GB, the maximum amount of processes you can run without swapping is 3. 
- `prefetch`: The number of tasks that are assigned to each worker whenever they are ready. It's easier to explain this with an analogy. Imagine you need to deliver 1000 products to customers and you have 4 couriers ready to deliver them. How inefficient would it be if every courier delivered one product at a time, returning to the warehouse whenever they complete a delivery? You would still parallelize work across 4 couriers, but would incur in massive synchronization costs (the extra time it takes for the courier to return to the warehouse each time). A smarter solution would be to assign a larger batch of deliveries to each courier (e.g. 50) so that they would have to return to the warehouse less often. 
- `keep_order`: Sometimes, the order in which the operations are performed is not that relevant. Executing tasks out-of-order requires even less synchronization, usually resulting in faster overall execution.

Let's see an example of how a `Grabber` works. 
!!! example

    We want to iterate through a sequence of objects that, whenever indexed, performs some expensive operation. In this example we simulate it using a `time.sleep()`.

    Here is our `SlowSequence`:
    
    ``` py
    class SlowSequence(Sequence[int]):
    """Return the numbers in range [A, B), slowly."""

        def __init__(self, start: int, stop: int) -> None:
            self._start = start
            self._stop = stop

        def __len__(self) -> int:
            return self._stop - self._start

        @overload
        def __getitem__(self, idx: int) -> int: ...
        @overload
        def __getitem__(self, idx: slice) -> "SlowSequence": ...
        def __getitem__(self, idx: int | slice) -> "SlowSequence | int":
            # Let's not care about slicing for now
            if isinstance(idx, slice):
                raise NotImplementedError()

            # Raise if index is out of bounds
            if idx >= len(self):
                raise IndexError(idx)

            # Simulate some slow operation and return
            time.sleep(0.1)
            return idx + self._start
    ```

    Let's create an instance of `SlowSequence` and define a callback function that we need to call every time we iterate on it:

    ``` py
    def my_callback(index: int) -> None:
        print(f"Callback {index} invoked by process {os.getpid()}")

    # The sequence object of which we want to compute the sum
    sequence = SlowSequence(100, 200)
    ```

    Next, let's try to compute the total sum of a `SlowSequence` with a simple for loop.

    ``` py
    # Compute the sum, and measure total running time
    t1 = time.perf_counter()

    total = 0
    for i, x in enumerate(sequence):
        my_callback(i)
        total += x
    
    t2 = time.perf_counter()
    print("Time (s):", t2 - t1) # >>> Time (s): 10.008610311000666
    print("Total:", total)      # >>> Total: 14950
    ```

    The total running time is roughly 10s, checks out, since every iteration takes approximately 0.1s and we iterate 100 times. The PID printed by the callback function is always the same, since all computation is performed by a single process.

    Let's do the same but with with a `Grabber` with 5 concurrent workers

    ``` py
    t1 = time.perf_counter()
    
    total = 0
    grabber = Grabber(num_workers=5)
    with grabber(sequence, callback=my_callback) as par_sequence:
        for i, x in par_sequence:
            total += x
    
    t2 = time.perf_counter()
    print("Time (s):", t2 - t1) # >>> Time (s): 2.011717862998921
    print("Total:", total)      # >>> Total: 14950
    ```

    The total time is now near to 2s, the result of evenly splitting among 5 parallel workers. As you can see from the console, the callback is being called by 5 different PIDs:

    ```
    Callback 0 invoked by process 21219
    Callback 2 invoked by process 21220
    Callback 4 invoked by process 21221
    Callback 6 invoked by process 21222
    Callback 8 invoked by process 21223
    ...
    ```

    Important note: what `Grabber` parallelizes just the call to the `__getitem__` method and the callback function. The body of the for loop (the summation) is **not** parallelized and it's executed by the parent process.

## Sources and Sinks

`DatasetSource` and `DatasetSink` are the two base classes for every Pipewine action that respectively **reads** or **writes** datasets (or collections of datasets) from/to external storages.

A `DatasetSource` creates and returns instances of Pipewine `Datasets`.
A `DatasetSink` consumes instances of Pipewine `Datasets` 
 


### Underfolder

### Custom Formats

## Operators

### Built-in Operators

### Custom Operators

## Mappers

### Built-in Mappers

### Custom Mappers

