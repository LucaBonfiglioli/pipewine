# 🗃️ Data Model

## Overview

Pipewine **data model** is composed of three main abstractions: 

- **Dataset** - A **Sequence** of `Sample` instances, where "sequence" means an ordered collection that supports indexing, slicing and iteration.
- **Sample** - A **Mapping** of strings to `Item` instances, where "mapping" means a set of key-value pairs that supports indexing and iteration. 
- **Item** - An object that has access to the underlying data unit. E.g. images, text, structured metadata, numpy arrays, and whatever serializable object you may want to include in your dataset.

Plus, some lower level components that are detailed later on. You can disregard them for now:

- **Parser** - Defines how an item should encode/decode the associated data.
- **ReadStorage** - Defines how an item should access data stored elsewhere.

<iframe style="border:none" width="800" height="450" src="https://whimsical.com/embed/S8SpLwdz7iiZ9HZFsjjJYB@YbhPqJZAwhQ5dRk3XgrxbuF8o4HtB46rKFdcY"></iframe>

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

When dealing with mutable data structures inside your items, make sure you either:

- Access the data without applying changes.
- Create a copy of the data before applying in-place changes.

!!! danger

    Never do this!

    ``` py
    # Read the image from a sample
    image = sample["image"]()
    
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
    image = sample["image"]()
    
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

## Dataset

## Sample

## Item

## Parser

## ReadStorage
