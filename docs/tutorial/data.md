# 🗃️ Data Model

## Overview

In this section your will learn what are the main data abstractions upon which Pipewine is built, how to interact with them and extend them according to your needs.

Pipewine **data model** is composed of three main abstractions: 

- **Dataset** - A **Sequence** of `Sample` instances, where "sequence" means an ordered collection that supports indexing, slicing and iteration.
- **Sample** - A **Mapping** of strings to `Item` instances, where "mapping" means a set of key-value pairs that supports indexing and iteration. 
- **Item** - An object that has access to the underlying data unit. E.g. images, text, structured metadata, numpy arrays, and whatever serializable object you may want to include in your dataset.

Plus, some lower level components that are detailed later on. You can disregard them for now:

- **Parser** - Defines how an item should encode/decode the associated data.
- **Reader** - Defines how an item should access data stored elsewhere.

<iframe style="border:none" width="800" height="450" src="https://whimsical.com/embed/S8SpLwdz7iiZ9HZFsjjJYB@YbhPqJZAwhQ5dRk3XgrxbuF8o4HtB46rKFdcY"></iframe>



## Dataset

`Dataset` is the highest-level container and manages the following information:

- How many samples it contains
- In which order

It provides methods to access individual samples or slices of datasets, as in Python [slice](https://docs.python.org/3/glossary.html#term-slice).

!!! note

    A `Dataset` is an immutable Python [Sequence](https://docs.python.org/3/glossary.html#term-sequence), supporting all its methods.


All `Dataset` objects are [Generics](https://docs.python.org/3/library/typing.html#generics), meaning that they can be hinted with information about the type of samples they contain. This is especially useful if you are using a static type checker.

!!! example

    Example usage of a `Dataset` object:

    ``` py
    # Given a Dataset of MySample's 
    dataset: Dataset[MySample]

    # Supports len
    number_of_samples = len(dataset)
    
    # Supports indexing
    sample_0 = dataset[0]    # The type checker infers the type: MySample
    sample_51 = dataset[51]  # The type checker infers the type: MySample

    # Suppors slicing
    sub_dataset = dataset[10:20] # The type checker infers the type: Dataset[MySample]

    # Supports iteration
    for sample in dataset:
        ...
    ```

By default Pipewine provides two implementations of the `Dataset` interface: 

- `ListDataset`
- `LazyDataset` 

### ListDataset

A `ListDataset` is basically a wrapper around a Python `list`, such that, whenever indexed, the result is immediately available. 

To achieve this, it has two fundamental requirements:

1. All samples must be known at creation time.
2. All samples must be always loaded into memory.

Due to these limitations, it's rarely used in the built-in operations, since the lazy alternative `LazyDataset` combined with caching provides a better trade-off, but it may be handy to have when:

- The number of samples is small.
- Samples are lightweight (i.e. no images, 3d data, huge tensors etc...)

!!! example

    Example of how to construct a `ListDataset`:

    ``` py
    # Create a list of samples
    samples = [ ... ] 

    # Wrap it in a ListDataset
    dataset = ListDataset(samples)
    ```

Time complexity (N = number of samples):

- Creation - O(N) (including the construction of the list)
- Length - O(1)
- Indexing - O(1)
- Slicing - O(N)

### LazyDataset

The smarter alternative is `LazyDataset`, a type of `Dataset` that defers the computation of the samples as late as possible. That's right, when using a `LazyDataset` samples are created when it is indexed, using a user-defined function that is passed at creation time.  

This has some implications:

- Samples are not required to be known at creation time, meaning that you can create a `LazyDataset` in virually zero time.
- Samples are not required to be kept loaded into memory the whole time, meaning that the memory required by `LazyDataset` is constant.
- Constant-time slicing.
- The computatonal cost shifts to the indexing part, which now carries the burden of creating and returning samples. 

!!! example

    Let's see an example of how to create and use a `LazyDataset`: 

    ``` py
    # Define a function that creates samples from an integer index.
    def get_sample_fn(idx: int) -> Sample:
        print(f"Called with index: {idx}")
        
        sample = ... # Omitted 
        return sample

    # Create a LazyDataset of length 10
    dataset = LazyDataset(10, get_sample_fn)

    # Do some indexing
    sample_0 = dataset[0] # Prints 'Called with index: 0'
    sample_1 = dataset[1] # Prints 'Called with index: 1'
    sample_2 = dataset[2] # Prints 'Called with index: 2'

    # Indexing the same sample multiple times calls the function multiple times
    sample_1 = dataset[1] # Prints 'Called with index: 1'
    ```

!!! warning

    What if my function is **very expensive** to compute? Is `LazyDataset` going to call it every time the dataset is indexed?

    **Yes**, but that can be avoided by using Caches, which are not managed by the `LazyDataset` class.

Time complexity (N = number of samples):

- Creation - O(1)
- Length - O(1)
- Indexing - Depends on `get_sample_fn` and `index_fn`.
- Slicing - O(1)

## Sample

`Sample` is a mapping-like container of `Item` objects. If dataset were tables (as in a SQL database), samples would be individual rows. Contrary to samples in a dataset, items in a sample do not have any ordering relationship and instead of being indexed with an integer, they are indexed by key.

!!! note

    A `Sample` is an immutable Python [Mapping](https://docs.python.org/3/glossary.html#term-mapping), supporting all its methods.

!!! example

    Let's see an example on how to use a `Sample` object as a python mapping:

    ``` py
    # Given a Sample (let's not worry about its creation)
    sample: Sample

    # Get the number of items inside the sample
    number_of_items = len(sample) 

    # Retrieve an item named "image".
    # This does not return the actual image, but merely an Item that has access to it.
    # This will be explained in detail later.
    item_image = sample["image"] 
    
    # Retrieve an item named "metadata"
    item_metadata = sample["metadata"]

    # Iterate on all keys
    for key in sample.keys():
        ...

    # Iterate on all items
    for item in sample.values():
        ...
    
    # Iterate on all key-item pairs
    for key, item in sample.items():
        ...
    ```

In addition to all `Mapping` methods, `Sample` provides a set of utility methods to create modified copies (samples are **immutable**) where new items are added, removed or have their content replaced by new values.

!!! example

    Example showing how to manipulate `Sample` objects using utility methods:

    ``` py
    # Given a Sample (let's not worry about its creation)
    sample: Sample

    # Add/Replace the item named "image" with another item
    new_sample = sample.with_item("image", new_image_item)

    # Add/Replace multiple items at once
    new_sample = sample.with_items(image=new_image_item, metadata=new_metadata_item)
    
    # Replace the contents of the item named "image" with new data
    new_sample = sample.with_value("image", np.array([[[...]]]))

    # Replace the contents of multiple items at once
    new_sample = sample.with_values(image=np.array([[[...]]]), metadata={"foo": 42})

    # Remove one or more items
    new_sample = sample.without("image")
    new_sample = sample.without("image", "metadata") 

    # Remove everything but one or more items
    new_sample = sample.with_only("image")
    new_sample = sample.with_only("image", "metadata")
    
    # Rename items
    new_sample = sample.remap({"image": "THE_IMAGE", "metadata": "THE_METADATA"})
    ```

In contrast with `Datasets`, pipewine does not offer a lazy version of samples, meaning that the all items are always kept memorized. Usually, you want to keep the number of items per sample bound to a constant number.

Pipewine provides two main `Sample` implementations that differ in the way they handle typing information.

### TypelessSample

The most basic type of `Sample` is `TypelessSample`, akin to the old Pipelime `Sample`. 
This class is basically a wrapper around a dictionary of items of unknown type.

When using `TypelessSample` it's your responsibility to know what is the type of each item, meaning that if you access an item you then have to cast it to the expected type.

With the old Pipelime, this quickly became a problem and lead to the creation of `Entity` and `Action` classes, that provide a type-safe alternative, but unfortunately integrate poorly with the rest of the library, failing to completely remove the need for casts or `type: ignore` directives. 

!!! example
    
    The type-checker fails to infer the type of the retrieved item:
    ``` py
    sample = TypelessSample(**dictionary_of_items)

    # When accessing the "image" item, the type checker cannot possibly know that the 
    # item named "image" is an item that accesses an image represented by a numpy array.
    image_item = sample["image"]

    # Thus the need for casting (type-unsafe)
    image_data = cast(np.ndarray, image_item())
    ```

Despite this limitation, `TypelessSample` allow you to use Pipewine in a quick-and-dirty way that allows for faster experimentation without worrying about type-safety.

!!! example

    At any moment, you can convert any `Sample` into a `TypelessSample`, dropping all typing information by calling the `typeless` method:

    ``` py
    sample: MySample

    # Construct a typeless copy of the sample
    tl_sample = sample.typeless()
    ```

### TypedSample

`TypedSample` is the type-safe alternative for samples. It allows you to construct samples that retain information on the type of each item contained within them, making your static type-checker happy.

`TypedSample` on its own does not do anything, to use it you always need to define a class that defines the names and the type of the items. This process is very similar to the definition of a Python dataclass, with minimal boilerplate.

What you get in return:

- No need for `t.cast` or `type: ignore` directives that make your code cluttered and error-prone.
- The type-checker will complain when something is wrong with the way you use your `TypedSample`, effectively preventing many potential bugs.
- Intellisense automatically suggests field and method names for auto-completion.
- Want to rename an item? Any modern IDE is able to quickly rename all occurrences of a `TypedSample` field without breaking anything.

!!! example

    Example creation and usage of a custom `TypedSample`:

    ``` py
    class MySample(TypedSample):
        image_left: Item[np.ndarray]
        image_right: Item[np.ndarray]
        category: Item[str]

    my_sample = MySample(
        image_left=image_left_item,
        image_right=image_right_item,
        category=category_item,
    )

    image_left_item = my_sample.image_left # Type-checker infers type Item[np.ndarray]
    image_left_item = my_sample["image_left"] # Equivalent type-unsafe

    image_right_item = my_sample.image_right # Type-checker infers type Item[np.ndarray]
    image_right_item = my_sample["image_right"] # Equivalent type-unsafe

    category_item = my_sample.category # Type-checker infers type Item[str]
    category_item = my_sample["category"] # Equivalent type-unsafe
    ```

!!! warning

    Beware of naming conflicts when using `TypedSample`. You should avoid item names conflicting with the methods of the `Sample` class.  

## Item

`Item` objects represent a single serializable unit of data. They are not the data itself, instead, they only have access to the underlying data.

Items do not implement any specific Python abstract type, since they are at the lowest level of the hierarchy and do not need to manage any collection of objects.

All items can be provided with typing information about the type of the data they have access to. This enables the type-checker to automatically infer the type of the data when accessed. 

All `Item` objects have a `Parser` inside of them, an object that is responsible to encode/decode the data when reading or writing. These `Parser` objects are detailed later on.

Furthermore, items can be flagged as "shared", enabling Pipelime to perform some optimizations when reading/writing them, but essentially leaving their behavior unchanged.


!!! example

    Example usage of an `Item`:

    ``` py
    # Given an item that accesses a string
    item: Item[str]
    
    # Get the actual data by calling the item ()
    actual_data = item()

    # Create a copy of the item with data replaced by something else
    new_item = item.with_value("new_string")

    # Get the parser of the item
    parser = item.parser

    # Create a copy of the item with another parser
    new_item = item.with_parser(new_parser)

    # Get the sharedness of the the item
    is_shared = item.is_shared

    # Set the item as shared/unshared
    new_item = item.with_sharedness(True)
    ```

Pipewine provides three `Item` variants, that differ in the way data is accessed or stored.

<iframe style="border:none" width="800" height="450" src="https://whimsical.com/embed/S8SpLwdz7iiZ9HZFsjjJYB@NKBbAEvLSyiJZQveGiXLMroxi8D1tPZ73"></iframe>

### MemoryItem

`MemoryItem` instances are items that directly contain data they are associated with. Accessing data is immediate as it is always loaded in memory and ready to be returned.

!!! tip

    Use `MemoryItem` to contain "new" data that is the result of a computation. E.g. the output of a complex DL model.

!!! example

    To create a `MemoryItem`, you just need to pass the data as-is and the `Parser` object:

    ``` py
    # Given a numpy array representing an image that is the output of an expensive
    # computation
    image_data = np.array([[[...]]])

    # Create a MemoryItem that contains the data and explicitly tells Pipewine to always
    # encode the data as a JPEG image.
    my_item = MemoryItem(image_data, JpegParser())
    ```

### StoredItem

`StoredItem` instances are items that point to external data stored elsewhere. Upon calling the item, the data is read from the storage, parsed and returned. 

`StoredItem` objects use both `Parser` and `Reader` objects to retrieve the data. A `Reader` is an object that exposes a `read` method that returns data as bytes.

Currently Pipewine provides a `Reader` for locally available files called `LocalFileReader`, that essentially all it does is `open(path, "rb").read()`.

!!! tip

    Use `StoredItem` to contain data that is yet to be loaded. E.g. when creating a dataset that reads from a DB, do not perform all the loading upfront, use `StoredItem` to lazily load the data only when requested.

!!! example

    To create a `StoredItem`, you need to 

    ``` py
    # The reader object responsible for reading the data as bytes
    reader = LocalFileReader(Path("/my/file.png"))

    # Create a StoredItem that is able able to read and parse the data when requested.
    my_item = StoredItem(reader, PngParser())
    ```

!!! warning

    Contrary to old Pipelime items, `StoredItem` do not offer any kind of automatic caching mechanism: if you retrieve the data multiple times, you will perform a full read each time. 

    To counteract this, you need to use Pipewine cache operations. 
 

### CachedItem

`CachedItem` objects are items that offer a caching mechanism to avoid calling expensive read operations multiple times when the underlying data is left unchanged. 

!!! warning

    Caching everything at the item level is an exceptionally bad idea that is guaranteed to fill up your system memory in no time. Instead of using `CachedItem` directly, use Pipewine `CacheOp` operations that provide a smarter mechanism that mixes sample-level and item-level caches. 

    This is an important lesson learnt from the old Pipelime library, where caching was always done at the item level and was enabled by default. 


To create a `CachedItem`, you just need to pass an `Item` of your choice to the `CachedItem` constructor.

!!! example

    Example usage of a `CachedItem`:

    ``` py
    # Suppose we have an item that reads a high resolution BMP image from an old HDD. 
    reader = LocalFileReader(Path("/extremely/large/file.bmp"))
    item = StoredItem(reader, BmpParser())
    
    # Reading data takes ages, and does not get faster if done multiple times.
    data1 = item() # Slow
    data2 = item() # Slow
    data3 = item() # Slow

    # With CachedItem, we can memoize the data after the first access, making subsequent
    # accesses immediate
    cached_item = CachedItem(item)

    data1 = cached_item() # Slow
    data2 = cached_item() # Fast
    data3 = cached_item() # Fast
    ```

## Parser

Pipewine `Parser` objects are responsible for implementing the serialization/deserialization functions for data:

- `parse` transforms bytes into python objects of your choice.
- `dump` transforms python objects into bytes.

### Built-in Parsers

Pipewine has some built-in parsers for commonly used data encodings: 

- `PickleParser`: de/serializes data using [Pickle](https://docs.python.org/3/library/pickle.html), a binary protocol that can be used to de/serialize most Python objects. Key pros/cons:
    
    - ✅ `pickle` can efficiently serialize pretty much any python object.
    - ❌ `pickle` is not secure: you can end up executing malicious code when reading data. 
    - ❌ `pickle` only works with Python, preventing interoperability with other systems.
    - ❌ There are no guarantees that `pickle` data written today can be correctly read by future python interpreters.  
  
- `JSONParser` and `YAMLParser` de/serializes data using [JSON](https://www.json.org/json-en.html) or [YAML](https://yaml.org/), two popular human-readable data serialization languages that support tree-like structures of data that strongly resemble Python builtin types.

    - ✅ Both JSON and YAML are interoperable with many existing systems.
    - ✅ Both JSON and YAML are standard formats that guarantee backward compatibility.
    - ⚠️ JSON and YAML only support a limited set of types such as `int`, `float`, `str`, `bool`, `dict`, `list`. 
    - ✅ `JSONParser` and `YAMLParser` interoperate with [pydantic](https://docs.pydantic.dev/latest/) `BaseModel` objects, automatically calling pydantic parsing, validation and dumping when reding/writing. 
    - ❌ Both JSON and YAML trade efficiency off for human readability. You may want to use different formats when dealing with large data that you don't care to manually read.

- `NumpyNpyParser` de/serializes numpy arrays into binary files. 

    - ✅ Great with dealing with numpy arrays of arbitrary shape and type
    - ❌ Only works with Python and Numpy.
    - ❌ Does not apply any compression to data, resulting in very large files.

- `TiffParser` de/serializes numpy arrays into [TIFF](https://www.loc.gov/preservation/digital/formats/fdd/fdd000022.shtml) files.

    - ✅ Great with dealing with numpy arrays of arbitrary shape and type.
    - ✅ Produces files that can be read outside of Python.
    - ✅ Applies [zlib](https://zlib.net/) lossless compression to reduce the file size. 

- `BmpParser` de/serializes numpy arrays into [BMP](https://en.wikipedia.org/wiki/BMP_file_format) files.

    - ⚠️ Only supports grayscale, RGB and RGBA uint8 images.
    - ❌ Does not apply any compression to data, resulting in very large files.
    - ✅ Fast de/serialization.
    - ✅ Lossless.

- `PngParser` de/serializes numpy arrays into [PNG](http://libpng.org/pub/png/) files.

    - ⚠️ Only supports grayscale, RGB and RGBA uint8 images.
    - ✅ Produces smaller files due to image compression. 
    - ❌ Slow de/serialization.
    - ✅ Lossless.

- `JpegParser` de/serializes numpy arrays into [JPEG](https://jpeg.org/) files.

    - ⚠️ Only supports grayscale and RGB uint8 images.
    - ✅ Produces very small files due to image compression. 
    - ✅ Fast de/serialization.
    - ❌ Lossy.

### Custom Parsers

With Pipewine you are not limited to use the built-in Parsers, you can implement your own and use it seamlessly as if it were provided by the library.

!!! example

    Let's create a `TrimeshParser` that is able to handle 3D meshes using the popular library [Trimesh](https://trimesh.org/index.html)

    ``` py
    class TrimeshParser(Parser[tm.Trimesh]):
        def parse(self, data: bytes) -> tm.Trimesh:
            # Create a binary buffer with the binary data
            buffer = io.BytesIO(data)

            # Read the buffer and let trimesh load the 3D mesh object
            return tm.load(buffer, file_type="obj")

        def dump(self, data: tm.Trimesh) -> bytes:
            # Create an empty buffer
            buffer = io.BytesIO()

            # Export the mesh to the buffer
            data.export(buffer, file_type="obj")

            # Return the contents of the buffer
            return buffer.read()

        @classmethod
        def extensions(cls) -> Iterable[str]:
            # This tells pipewine that it can automatically use this parses whenever a 
            # file with .obj extension is found and needs to be parsed.
            return ["obj"]

    ```


## Immutability

All data model types are **immutable**. Their inner state is hidden in private fields and methods and should never be modified in-place. Instead, they provide public methods that return copies with altered values, leaving the original object intact.

With immutability, a design decision inherited by the old Pipelime, we can be certain that every object is in the correct state everytime, since it cannot possibly change, and this prevents many issues when the same function is run multiple times, possibly in non-deterministic order.

!!! example

    Let's say you have a sample containing an item named `image` with an RGB image. You want to resize the image reducing the resolution to 50% of the original size.

    To change the image in a sample, you need to create a **new sample** in which the `image` item contains the resized image.

    ``` py
    def half_res(image: np.ndarray) -> np.ndarray:
        # Some code that downscales an image by 50%
        ...

    # Read the image (more details later)
    image = sample["image"]()

    # Downscale the image
    half_image = half_res(image)

    # Create a new sample with the new (downscaled) image
    new_sample = sample.with_value("image", half_image)
    ```

    At the end of the snippet above, the `sample` variable will still contain the original full-size image. Instead, `new_sample` will contain the new resized image.

There are only two exceptions to this immutability rule:

1. **Caches**: They need to change their state to save time when the result of a computation is already known. Since all other data is immutable, caches never need to be invalidated.
2. **Inner data**: While all pipewine data objects are immutable, this may not be true for the data contained within them. If your item contains mutable objects, you are able to modify them implace. **But never do that!** 

Python, unlike other languages, has no mechanism to enforce read-only access to an object, the only way to do so would be to perform a deep-copy whenever an object is accessed, but that would be a complete disaster performance-wise.

So, when dealing with mutable data structures inside your items, make sure you either:

- Access the data without applying changes.
- Create a deep copy of the data before applying in-place changes.

!!! danger

    Never do this!

    ``` py
    # Read the image from a sample
    image = sample["image"]() # <-- We need to call the item () to retrieve its content
    
    # Apply some in-place changes to image
    image += 1
    image *= 0.9 
    image += 1   

    # Create a new sample with the new image
    new_sample = sample.with_value("image", image)
    ```

    The modified image will be present both in the old and new sample, violating the immutability rule.

!!! success

    Do this instead!

    ``` py
    # Read the image from a sample
    image = sample["image"]() # <-- We need to call the item () to retrieve its content
    
    # Create a copy of the image with modified data
    image = image + 1

    # Since image is now a copy of the original data, you can now apply all 
    # the in-place changes you like now. 
    image *= 0.9 # Perfectly safe
    image += 1   # Perfectly safe

    # Create a new sample with the new image
    new_sample = sample.with_value("image", image)
    ```

    The modified image will be present only in the new sample.