# ⚙️ Actions

## Overview

In this section you will learn what are the main operational abstractions that define how Pipewine transforms data, how to interact with them and how to build re-usable building blocks for your data pipelines.

!!! note

    This section assumes you already know the basics on the way the data model is structured. If you haven't already, go take a look at the [Data Model](data.md) section. 

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

    You may already have encountered the `Bundle` type without noticing when we introduced the [TypedSample](data.md#typedsample). `TypedSample` is a `Bundle[Item]`.

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
- It does not prevent you from implementing optimized multi-threaded components in other languages. Think about it: whenever you use tools like Numpy inside a Pipewine action, you are doing exactly this. 

### Grabber

To parallelize tasks with Pipewine you can use a `Grabber` object: a simple wrapper around a multiprocessing `Pool` of workers iterate through an ordered sequence of objects.

!!! note

    `Grabber` objects can work on arbitrary sequences of objects, provided they are serializable.

The `Grabber` object also does a few nice things for you:

- Manages the lifecycle of the underlying pool of processes.
- Handles exceptions freeing resources and returning the errors transparently to the parent process.
- Automatically invokes a callback function whenever a task is completed. The callback is run by the individual workers.

When creating a `Grabber` you need to choose three main parameters that govern the execution:

- `num_workers`: The number of concurrent workers. Ideally, in a magic world where all workload is perfectly distributed and communication overhead is zero, the total time is proportional to `num_jobs / num_workers`, so the higher the better. In reality, there are many factors that make the returns of adding more processes strongly diminishing, and sometimes even negative:
    - Work is not perfectly distributed and it's not complete until the slowest worker hasn't finished. 
    - Processes are expensive to create. With small enough workloads, a single process may actually be faster than a concurrent pool.
    - Everytime an object is passed from one process to the other it needs to be serialized and then de-serialized. This can become quite expensive when dealing with large data structures.
    - Processes need synchronization mechanisms that temporarily halt the computation.
    - Sometimes the bottleneck is not the computation itself: if a single process reading data from the network completely saturates your bandwidth, adding more processes won't fix the issue.
    - Sometimes multiprocessing can be much slower than single processing. A typical example is when concurrently writing to a mechanical HDD, causing it to waste a lot of time going back and forth the writing locations.  
    - Your code may already be parallelized. Whenever you use libraries like Numpy, PyTorch, OpenCV (and many more), you are calling C/C++ bindings that run very efficient multi-threaded code outside of the Python interpreter, or even code that runs on devices other than your CPU (e.g. CUDA-enabled GPUs). Adding multiprocessing in these cases will only add overhead costs to something that already uses your computational resources nearly optimally.
    - Due to memory constraints the number of processes you can keep running may be limited to a small number. E.g. if each process needs to keep loaded a 10GB chunk of data and the available memory is just 32GB, the maximum amount of processes you can run without swapping is 3. Adding a 4th process will make the system start swapping, greatly deteriorating the overall execution speed.
- `prefetch`: The number of tasks that are assigned to each worker whenever they are ready. It's easier to explain this with an analogy. Imagine you need to deliver 1000 products to customers and you have 4 couriers ready to deliver them. How inefficient would it be if every courier delivered one product at a time, returning to the warehouse whenever they complete a delivery? You would still parallelize work across 4 couriers, but would also incur in massive synchronization costs (the extra time it takes for the courier to return to the warehouse each time). A smarter solution would be to assign a larger batch of deliveries to each courier (e.g. 50) so that they would have to return to the warehouse less frequently. 
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
            # Let's not worry about slicing for now
            if isinstance(idx, slice):
                raise NotImplementedError()

            # Raise if index is out of bounds
            if idx >= len(self):
                raise IndexError(idx)

            # Simulate a slow operation and return
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

    Important note: what `Grabber` just parallelizes the `__getitem__` method and the callback function. The body of the for loop (the summation) is **not** parallelized and it's executed by the parent process.

## Sources and Sinks

`DatasetSource` and `DatasetSink` are the two base classes for every Pipewine action that respectively reads or writes datasets (or collections of datasets) from/to external storages.

Pipewine offers built-in support for *Underfolder* datasets, a flexible dataset format inherited by Pipelime that works well with many small-sized multi-modal datasets with arbitrary content. 

In order to provide a simple yet effective alternative to just read a dataset of images from a directory tree, pipewine also provides you the "Images Folder" format.

You are strongly encouraged to create custom dataset sources and sinks for whatever format you like.

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

### Images Folders

Images folder are a much simpler alternative when the input dataset only consists of unstructured image files contained in a directory tree. 

By default, `ImagesFolderSource` looks at image files inside the specified directory
path. If used with the `recursive` argument set to true, it will also look recursively
into subfolders. The image files are sorted in lexicographic order to ensure that the result is always
deterministic.

Contrary to the Underfolder format, which can accept and parse any type of sample, this source forces its samples to be `ImageSample` objects, i.e. samples that only contain an `image` item represented by a numpy array.

!!! example

    Basic `ImagesFolderSource` usage:

    ``` py
    # Create the source object from an existing directory Path.
    path = Path("tests/sample_data/images_folders/folder_1")
    source = ImagesFolderSource(path, recursive=True)

    # Call the source object to create a new Dataset instance.
    dataset = source()

### Custom Formats

You can add custom implementations of the `DatasetSource` and `DatasetSink` interfaces that allow reading and writing datasets with custom formats. To better illustrate how to do so, this section will walk you through an example use case of a source and a sink for folders containing JPEG image files.

!!! warning

    In case you really need to read images from an unstructured folder, just use the `ImagesFolderSource` class, without having to write it yourself. This is just a simplified example to help you understand how these components work.

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

We have seen how to read and write datasets using dataset sources and sinks, now it's time to see how to apply operations that transform datasets into other datasets. This is done by implementations of the `DatasetOperator` base class. 

Similarly to sources and sinks, operators must statically declare the type of data they accept as input and the one they return, for a total of 25 (5x5) possible combinations of input and output types.

!!! example

    For example, a dataset operator that splits a single dataset into exactly 3 parts, should specify:
    
    - Input type: `Dataset`
    - Output type: `tuple[Dataset, Dataset, Dataset]`

Operators are constructed as plain Python objects, then they behave like a Python `Callable` accepting and returning either a dataset or a collection of datasets.

!!! example

    Here is an example of how to use a `DatasetOperator` that concatenates two or more datasets into a single one.

    ``` py
    # Given N datasets
    dataset1: Dataset
    dataset2: Dataset
    dataset3: Dataset

    # Construct the operator object
    concatenate = CatOp()

    # Call the object
    full_dataset = concatenate([dataset1, dataset2, dataset3])
    ```

Just like most Pipewine components, dataset operators can either be:

- Eager: performing all the computation when called and returning dataset/s containing the computation results. This is similar to the way old Pipelime's `PipelimeCommand` objects behave.
- Lazy: the call immediately returns a `LazyDataset` instance that performs the actual computation when requested. 

!!! tip

    In reality, it often makes sense to use a mix of the two approaches. You can perform some eager computation upfront then return a lazy dataset that uses the result of the previous computation.  

### Built-in Operators

Pipewine has some useful generic operators that are commonly used in many workflows. Here is a list of the currently available built-in operators with a brief explanation. More in-depth documentation in the API reference.

**Iteration operators:** operators that operate on single datasets changing their length or the order of samples.

- `SliceOp`: return a slice (as in Python slices) of a dataset.
- `RepeatOp`: replicate the samples of a dataset N times.
- `CycleOp`: replicate the samples of a dataset until the desired number of samples is reached.
- `IndexOp`: select a sub-sequence of samples of a dataset with arbitrary order.
- `ReverseOp`: invert the order of samples in a dataset (same as `SliceOp(step=-1)`).
- `PadOp`: extend a dataset to a desired length by replicating the i-th sample.

**Merge operators:** operators that merge a collection of datasets into a single one.

- `CatOp`: concatenate one or more datasets into a single dataset preserving the order of samples.
- `ZipOp`: zip two or more dataset with the same length by merging the contents of the individual samples.

**Split operators:** operators that split a single dataset into a collection of datasets.

- `BatchOp`: split a dataset into many datasets of the desired size.
- `ChunkOp`: split a dataset into a desired number of chunks (of approximately the same size).
- `SplitOp`: split a dataset into an arbitrary amount of splits with arbitrary size.

**Functional operators:** operators that transform datasets based on the result of a user-defined function.

- `FilterOp`: keep only (or discard) samples that verify an arbitrary predicate.
- `GroupByOp`: split a dataset grouping together samples that evaluate to the same value of a given function.
- `SortOp`: sort a dataset with a user-defined sorting key function.
- `MapOp`: apply a user-defined function (`Mapper`) to each sample of a dataset.

**Random operators:** operators that apply non-deterministic random transformations.

- `ShuffleOp`: sort the samples of a dataset in random order.

**Cache operators:** operators that do not apply any transformation to the actual data, bu only change the way they are accessed.

- `CacheOp`: adds a caching layer that memorizes samples to avoid computing them multiple times.

### Custom Operators

As with sources and sinks, you can implement your own operators that manipulate data as you wish. To help you understand how to do so, this section will walk you through the implementation of an example operator that normalizes images.

To implement the dataset operator:

1. Inherit from `DatasetOperator` and specify both the input and output types. You can choose to implement operators that work with specific types of data or to allow usage with arbitrary types.
2. [Optional] Implement an `__init__` method.
3. Implement the `__call__` method, accepting and returning the previously specified types of data. 

!!! example

    Here is the code for the `NormalizeOp`, an example operator that applies a channel-wise z-score normalization to all images of a dataset. 

    The implementation is realatively naive: reading and stacking all images in a dataset is not a good strategy memory-wise. In a real-world scenario, you want to compute mean and standard deviation using constant-memory approaches. However, for the sake of this tutorial we can disregard this aspect and focus on other aspects of the code.

    Important things to consider:

    - Since we want to be able to use this operator with many types of dataset, we inherit from `DatasetOperator[Dataset, Dataset]`, leaving the type of dataset unspecified for now.
    - The `__call__` method has a typevar `T` that is used to tell the type-checker that the type of samples contained in the input dataset is preserved in the output dataset, allowing us to use this operator with any dataset we want.
    - In the first part of the `__call__` method, we eagerly compute mu and sigma vectors iterating over the whole dataset.
    - In the second part of the `__call__` method we return a lazy dataset instance that applies the normalization.

    ``` py
    class NormalizeOp(DatasetOperator[Dataset, Dataset]):
        def __init__(self, grabber: Grabber | None = None) -> None:
            super().__init__()
            self._grabber = grabber or Grabber()

        def _normalize[
            T: Sample
        ](self, dataset: Dataset[T], mu: np.ndarray, sigma: np.ndarray, i: int) -> T:
            # Get the image of the i-th sample
            sample = dataset[i]
            image = sample["image"]().astype(np.float32)

            # Apply normalization then bring values back to the [0, 255] range, 
            # clipping values below -sigma to 0 and above sigma to 255.
            image = (image - mu) / sigma
            image = (image * 255 / 2 + 255 / 2).clip(0, 255).astype(np.uint8)

            # Replace the image item value
            return sample.with_value("image", image)

        def __call__[T: Sample](self, x: Dataset[T]) -> Dataset[T]:
            # Compute mu and sigma on the dataset (eager)
            all_images = []
            for _, sample in self.loop(x, self._grabber, name="Computing stats"):
                all_images.append(sample["image"]())
            all_images_np = np.concatenate(all_images)
            mu = all_images_np.mean((0, 1), keepdims=True)
            sigma = all_images_np.std((0, 1), keepdims=True)

            # Lazily apply the normalization with precomputed mu and sigma
            return LazyDataset(len(x), partial(self._normalize, x, mu, sigma))
    ```

Custom dataset operators can be registered to the Pipewine CLI to allow you to apply custom transformations to your datasets using a common CLI. This is covered in the [CLI](cli.md) tutorial. 

## Mappers

Pipewine `Mapper` objects (essentially the same as Pipelime Stages), allow you to quickly implement dataset operators by defining a function that transforms individual samples.

This allows you to write way less code when all the following conditions apply:

- The operation accepts and returns a single dataset.
- The input and output datasets have the same length.
- The i-th sample of the output dataset can be computed as a pure function of the i-th sample of the input dataset.

!!! danger

    Never use mappers when the function that transforms samples is stateful. Not only Pipewine does not preserve the order of samples when calling mappers, but the execution may be run concurrently in different processes that do not share memory.
    
    In these cases, use a `DatasetOperator` instead of a `Mapper`.


Mappers must be used in combination with `MapOp`, a built-in dataset operator that lazily applies a mapper to every sample of a dataset.

!!! example

    Example usage of a mapper that renames items in a dataset:

    ``` py
    # Given a dataset
    dataset: Dataset

    # Rename all items named "image" into "my_image"
    op = MapOp(RenameMapper({"image": "my_image"}))

    # Apply the mapper
    dataset = op(dataset)
    ```
 
If you need to apply multiple mappers, instead of applying them individually using multiple `MapOp`, you should compose the mappers into a single one using the built-in `ComposeMapper` and then apply it with a single `MapOp`.

All mappers are statically annotated with two type variables representing the type of input and output samples they accept/return, enabling static type checking. E.g. `Mapper[SampleA, SampleB]` accepts samples of type `SampleA` and returns samples of type `SampleB`. When composed in a `ComposeMapper`, it will automatically detect the input and output type from the sequence of mappers it is constructed with.

!!! example

    Here, the static type-checker automatically infers type `Mapper[SampleA, SampleE]` for the variable `composed`:

    ``` py
    mapper_ab: Mapper[SampleA, SampleB]
    mapper_bc: Mapper[SampleB, SampleC]
    mapper_cd: Mapper[SampleC, SampleD]
    mapper_de: Mapper[SampleD, SampleE]

    composed = ComposeMapper((mapper_ab, mapper_bc, mapper_cd, mapper_de))
    ```


### Built-in Mappers

Pipewine has some useful generic mappers that are commonly used in many workflows. Here is a list of the currently available built-in mappers with a brief explanation. More in-depth documentation in the API reference.

**Key transformation mappers:** modify the samples by adding, removing and renaming keys.

- `DuplicateItemMapper`: create a copy of an existing item and give it a different name.
- `FormatKeysMapper`: rename items using a format string.
- `RenameMapper`: rename items using a mapping from old to new keys.
- `FilterKeysMapper`: keep or discard a subset of items.

**Item transformation mappers:** modify item properties such as parser and sharedness.

- `ConvertMapper`: change the parser of a subset of items, e.g. convert PNG to JPEG.
- `ShareMapper`: change the sharedness of a subset of items.

**Cryptography mappers:** currently contains only `HashMapper`.

- `HashMapper`: computes a secure hash of a subset of items, useful to perform deduplication or integrity checks.

**Special mappers:**

- `CacheMapper`: converts all items to `CachedItem`. 
- `ComposeMapper`: composes many mappers into a single object that applies all functions sequentially. 

### Custom Mappers

To implement a `Mapper`:

1. Inherit from `Mapper` and specify both the input and output sample types.
2. [Optional] Implement an `__init__` method.
3. Implement the `__call__` method, accepting an integer and the input sample, and returning the output sample.

!!! danger

    Remember: mappers are stateless. Never use object fields to store state between subsequent calls.

!!! example 

    In this example we will implement a mapper that inverts the colors of an image.

    ``` py
    class ImageSample(TypedSample):
        image: Item[np.ndarray]

    class InvertRGBMapper(Mapper[ImageSample, ImageSample]):
        def __call__(self, idx: int, x: ImageSample) -> ImageSample:
            return x.with_value("image", 255 - x.image())
    ```

Custom mappers can be registered to the Pipewine CLI to allow you to apply custom transformations to your datasets using a common CLI. This is covered in the [CLI](cli.md) tutorial. 