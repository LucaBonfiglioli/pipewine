# 💠 Overview

## High Level

**Pipewine** provides you with tools to help decouple what you do with data from the way data is represented and stored. It does so by providing a set of abstractions for many aspects of your data pipeline:

- `Dataset`, `Sample`, `Item` define how the data is structured, how many data samples are there, in which order, what is their content, how are they accessed etc...
- More low-level abstractions such as `Parser` and `ReadStorage` define how data is encoded and stored. 
- `DatasetSource`, `DatasetSink`, `DatasetOperator` define how the data is read, written and transformed, and consistitute the base building blocks for workflows.
- `Workflow` defines how a set of operators are interconnected. They can be seen as DAGs (Directed Acyclic Graph) in which nodes are sources, sinks or operators, and edges are datasets. 

All of these components are designed to allow the user to easily create custom implementations that can be seamlessly integrated with all the built-in blocks.

By doing so, Pipewine (much like Pipelime) encourages you to write components that are likely to be highly re-usable.

<iframe style="border:none" width="800" height="450" src="https://whimsical.com/embed/S8SpLwdz7iiZ9HZFsjjJYB@6HYTAunKLgTUesrFqzeTZY9X3YMh3oKU2qNqAphr4F1BXn7"></iframe>

## Extendibility

**Pipewine** is **completely agnostic** on the following aspects of your data:

- **Storage location**: you can store data anywhere you want, on the file system, on a DB of your choice, on the device memory, on a remote source. You just need to implement the necessary components. 
- **Data encoding**: By default Pipewine supports some popular image encodings, JSON/YAML metadata, numpy encoding for array data and Pickle encoding for generic python objects. You can easily add custom encodings to read/write data as you like.
- **Data format**: By default Pipewine supports the same built-in dataset format as Pipelime, a file system based format called "Underfolder" that is flexible to most use-cases but has a few limitations. Dataset formats are highly dependent on the application, thus Pipewine allows you to fully take control on how to structure your datasets.
- **Data operators**: As mentioned previously, you can define custom operators that do all sorts of things with your data. Built-in operators cover some common things you may want to do at some point such as concatenating two or more datasets, filtering samples based on a criterion, splitting datasets into smaller chunks, apply the same function (called `Mapper`) to all samples of a dataset.  

## A Note on Performance

Pipewine is a python package and it's currently 100% python, therefore it's certainly going to be orders of magnitude slower than it could be if written in another language.

Having said that, Pipewine still tries its best to maximize efficiency by leveraging:

- **Caching**: Results of computations can be cached to avoid being computed multiple times. This was also done by Pipelime, but they way cache works underwent many changes in the rewrite.
- **Parallelism**: Many operations are automatically run in parallel with a multi-processing pool of workers. 
- **Linking**: When writing to file system, Pipewine automatically attempts to leverage hard-links where possible to avoid serializing and writing the same file multiple times.
- **Vectorization**: Where possible, Pipewine uses Numpy to perform vectorized computation on batches of data, achieving better performance if compared to plain python code.

Furthermore, when performing complex operations such as image processing, inference with AI models, 3D data processing, the performance overhead of Pipewine will likely become negligible if compared to the complexity of the individual operations.

## A Note on Scalability

**Pipewine** - and its predecessor Pipelime - are meant to quickly let you manipulate data without either having to:

- Coding everything from scratch and come up with meaningful abstractions yourself. 
- Setting up complex and expensive frameworks that can run data pipelines on distributed systems with many nodes.

!!! warning

    If you are running data pipelines on petabytes of data, in distributed systems, with strong consistency requirements and the need for data replication at each step, Pipewine **is not** what you are looking for.

!!! success

    If you need to run data pipelines on small/medium datasets (in the order of gigabytes) and want a flexible tool to help you do that, then Pipewine **might be** what you are looking for.
