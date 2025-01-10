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
- `DatasetOperator` - A component that accepts and returns one or more `Dataset` objects.

In general, actions can accept and return one of the following:

- A single `Dataset` object.
- A `Sequence` of datasets.
- A `tuple` of datasets.
- A `Mapping` of strings to datasets.
- A `Bundle` of datasets.

Specific types of actions statically declare the type of inputs and outputs they accept/return to ensure that their type is always known at development time (as soon as you write the code).

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

As you may have noticed in the previous section, Pipewine allows you to choose both approaches, but when there is no clear reason to prefer the eager behavior, its components are implemented so that they behave lazily: `Item` only reads data when requested to do so, `LazyDataset` creates the actual `Sample` object upon indexing. As you will see, most Pipewine actions follow a similar pattern.

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

- `num_workers`: The number of concurrent workers. Ideally, in a magic world where all workload is perfectly distributed and communication overhead is zero, the total time is proportional to `num_jobs / num_workers`, so the higher the better. In reality, there are many factors that make the returns of adding more processes strongly diminishing, and sometimes even negative:
    - Work is not perfectly distributed and it's not complete until the slowest worker hasn't finished. 
    - Processes are expensive to create. With small enough workloads, a single process may actually be faster than a concurrent pool.
    - Everytime an object is passed from one process to the other it needs to be serialized and then de-serialized. This can become quite expensive when dealing with large data structures.
    - Processes need synchronization mechanisms that temporarily halt the computation.
    - Sometimes the bottleneck is not the computation itself: if a single process reading data from the network completely saturates your bandwidth, adding more processes won't fix the problem.
    - Sometimes multiprocessing can be much slower than single processing. A typical example is when concurrently writing to a mechanical HDD, causing it to waste a lot of time going back and forth the writing locations.  
    - Your code may already be parallelized. Whenever you use libraries like Numpy, PyTorch, OpenCV (and many more), you are calling C/C++ bindings that run very efficient multi-threaded code outside of the Python interpreter, or even code that runs on devices other than your CPU (e.g. CUDA-enabled GPUs). Adding multiprocessing in these cases will only add overhead costs to something that already uses your computational resources nearly optimally.
    - Due to memory constraints the number of processes you can keep running may be limited to a small number. E.g. if each process needs to keep loaded a 10GB chunk of data and the available memory is just 32GB, the maximum amount of processes you can run without swapping is 3. Adding a 4th process will make the system start swapping, greatly deteriorating the overall execution speed.
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

`DatasetSource` and `DatasetSink` are the two base classes for every Pipewine action that respectively reads or writes datasets (or collections of datasets) from/to external storages.

Pipewine offers built-in support for *Underfolder* datasets, a flexible dataset format inherited by Pipelime that works well with many small-sized multi-modal datasets with arbitrary content. 

While Underfolder is the only format supported by Pipewine (currently), you are strongly encouraged to create custom dataset sources and sinks for whatever format you like.

### Underfolder

An underfolder dataset is a directory located anywhere on the file system, with no constraint on its name. Every underfolder must contain a subfolder named `data`, plus some additional files.

Every file contained in the `data` subfolder corresponds to an item and must be named: `$INDEX_$KEY.$EXTENSION`, where:

- `$INDEX` is a non-negative integer number representing the index of the sample in the dataset, prefixed with an arbitrary number of `0` characters that are ignored. 
- `$KEY`  is a string representing the key of the item in the sample. Every name that can be used as a Python variable name is a valid key.
- `$EXTENSION` is the file extension you would normally use. Pipewine can only read files if there is a registered `Parser` class that supports its extension.  

Every file outside of the `data` folder (also called "root file") represents a shared item that every sample in the dataset will inherit.

!!! warning

    All sample indices must be contiguous: if a sample with index N is present, then all the samples from 0 to N-1 must also be present in the dataset.


<iframe style="border:none" width="800" height="450" src="https://whimsical.com/embed/S8SpLwdz7iiZ9HZFsjjJYB@or4CdLRbgroHUKdJDDwGHScPtCHyW8WkbahA1dJaH"></iframe>

Pros and cons of the Underfolder dataset format:

- ✅ No need to setup databases, making them extremely easy to create and access. You can even create them by renaming a bunch of files using your favourite file explorer or a shell script.
- ✅ You can easily inspect and edit the content of an underfolder dataset just by opening the individual files you are interested in with your favourite tools.
- ✅ It's a very flexible format that you can use in many scenarios. 
- ❌ Databases exist for a reason. Putting 100.000 files in a local directory and expecting it to be efficient is pure fantasy. You should avoid using underfolders when dealing with large datasets or when performance is critical.
- ❌ There is an issue when used with datasets where all items are shared (an edge case that's very rare in practice). In these cases, the original length of the dataset is lost after writing. Although fixing this issue is quite easy, it would break the forward-compatibility (reading data written by a future version of the library), so it will likely remain unfixed.

!!! example

    You can see an example of an Underfolder dataset [here](https://github.com/LucaBonfiglioli/pipewine/tree/0ffaa9cb0ee829f78bcdf56cbbe21110045a94d8/tests/sample_data/underfolders/underfolder_0). The dataset contains samples corresponding to the 26 letters of the english alphabet, with RGB images and metadata.

    Basic `UnderfolderSource` usage with no typing information:

    ``` py
    # Create the source object from an existing directory Path.
    path = Path("tests/sample_data/underfolders/underfolder_0")
    source = UnderfolderSource(path)

    # Call the source object to create a new Dataset instance.
    dataset = source()

    # Do stuff with the dataset
    sample = dataset[4]
    print(sample["image"]().reshape(-1, 3).mean(0))  # >>> [244.4, 231.4, 221.7]
    print(sample["metadata"]()["color"])  #            >>> "orange"
    print(sample["shared"]()["vowels"])  #             >>> ["a", "e", "i", "o", "u"]
    ```

    Fully typed usage:

    ``` py
    class LetterMetadata(pydantic.BaseModel):
        letter: str
        color: str

    class SharedMetadata(pydantic.BaseModel):
        vowels: list[str]
        consonants: list[str]

    class LetterSample(TypedSample):
        image: Item[np.ndarray]
        metadata: Item[LetterMetadata]
        shared: Item[SharedMetadata]

    # Create the source object from an existing directory Path.
    path = Path("tests/sample_data/underfolders/underfolder_0")
    source = UnderfolderSource(path, sample_type=LetterSample)

    # Call the source object to create a new Dataset instance.
    dataset = source()

    # Do stuff with the dataset
    sample = dataset[4]
    print(sample.image().reshape(-1, 3).mean(0))  # >>> [244.4, 231.4, 221.7]
    print(sample.metadata().color)  #               >>> "orange"
    print(sample.shared().vowels)  #                >>> ["a", "e", "i", "o", "u"]
    ```

    You can use `UnderfolderSink` to write any Pipewine dataset, even if it wasn't previously read as an underfolder.

    ``` py
    # Write the dataset with an underfolder sink
    output_path = Path(gettempdir()) / "underfolder_write_example"
    sink = UnderfolderSink(output_path)
    sink(dataset) # <-- Writes data to the specified path.
    ```

### Custom Formats

You can add custom implementations of the `DatasetSource` and `DatasetSink` interfaces that allow reading and writing datasets with custom formats. To better illustrate how to do so, this section will walk you through an example use case of a source and a sink for folders containing JPEG image files.

Before you start, you should always think of the type of datasets you are going to read. Some formats may be flexible enough to support any kind of dataset, while others may be restricted to only a specific type. Pipewine gives you the tools to choose any of the two options in a type-safe way, but also to completely ignore all the typing part and to always return un-typed structures as it was with the old Pipelime.

!!! example

    In this example case it's very simple: we only want to read samples that contain a single item named "image" that loads as a numpy array:

    ``` py
    class ImageSample(TypedSample):
        image: Item[np.ndarray]
    ```

Next, let's implement the dataset source:

1. Inherit from `DatasetSource` and specify the type of data that will be read.
2. [Optional] Implement an `__init__` method. 
3. Implement the `__call__` method, accepting no arguments and returning an instance of chosen type of dataset. You can choose to load the whole dataset upfront (eager), or to return a `LazyDataset` that will load the samples only when needed.

!!! tip

    When overriding the `__init__` method, always remember to call `super().__init__()`, otherwise the object won't be correctly initialized.

!!! example

    ```py
    class ImagesFolderSource(DatasetSource[Dataset[ImageSample]]):
        def __init__(self, folder: Path) -> None:
            super().__init__()  # Always call the superclass constructor!
            self._folder = folder
            self._files: list[Path]

        def __call__(self) -> Dataset[ImageSample]:
            # Find all JPEG files in the folder in lexicographic order.
            jpeg_files = filter(lambda x: x.suffix == ".jpeg", self._folder.iterdir())
            self._files = sorted(list(jpeg_files))

            # Return a lazy dataset thet loads the samples with the _get_sample method
            return LazyDataset(len(self._files), self._get_sample)

        def _get_sample(self, idx: int) -> ImageSample:
            # Create an Item that reads a JPEG image from the i-th file.
            reader = LocalFileReader(self._files[idx])
            parser = JpegParser()
            image_item = StoredItem(reader, parser)

            # Return an ImageSample that only contains the image item.
            return ImageSample(image=image_item)
    ```

Next, let's implement the dataset sink, which is somewhat specular to the source:

1. Inherit from `DatasetSink` and specify the type of data that will be written.
2. [Optional] Implement an `__init__` method. 
3. Implement the `__call__` method, accepting an instance of the chosen type of dataset and returning nothing. 

!!! tip

    Since sinks are always placed at the end of pipelines, computation cannot be delayed any further, giving them no option but to loop over the data in an eager way inside the `__call__` method. We recommend doing this using the `self.loop` method with a grabber. Doing this has two advantages:

    1. It loops over the data using a grabber, meaning that if there are some lazy operations that still need to be computed, they can be run efficiently in parallel.
    2. It automatically invokes callbacks that send progress updates to whoever is listening to them, enabling live progress tracking in long-running jobs.

!!! example

    ``` py
    class ImagesFolderSink(DatasetSink[Dataset[ImageSample]]):
    # Inherit from DatasetSink and specify the type of data that will be written.
    # In this case, we only want to write a single dataset with a custom sample type.

        def __init__(self, folder: Path, grabber: Grabber | None = None) -> None:
            super().__init__()  # Always call the superclass constructor!
            self._folder = folder
            self._grabber = grabber or Grabber()

        def __call__(self, data: Dataset[ImageSample]) -> None:
            self._folder.mkdir(parents=True, exist_ok=True)

            # Compute the amount of 0-padding to preserve lexicographic order.
            zpadding = len(str(len(data)))

            # Iterate over the dataset and write each sample.
            for i, sample in self.loop(data, self._grabber, name="Writing"):
                fname = self._folder / f"{str(i).zfill(zpadding)}.jpeg"
                write_item_to_file(sample.image, fname)
    ```

Custom dataset sources and sinks can be registered to the Pipewine CLI to allow you to manipulate your own datasets using the same commands you would normally use for every other dataset format. This is covered in the [CLI](cli.md) tutorial. 

## Operators

### Built-in Operators

### Custom Operators

## Mappers

### Built-in Mappers

### Custom Mappers

