# ♻️ Workflows

## Overwiew

So far in this tutorial we always focused on simple cases where we only needed to apply a single operation to our data, with the only exception of a few cache-related examples where we saw the interactions and side-effects of two lazy operations applied sequentially. 

While applying a single operation to your data may be useful in some situations, more often than not you will need to apply multiple operations in a repeatable and organized way. One way of doing this is to write a python script that calls the right operations in the right order and that's it, but with a little extra effort you can turn your script into a Pipewine `Workflow`: a *Directed Acyclic Graph* (DAG) where nodes represent Actions (either a source, operator or sinks) and edges represent the dependencies between them.

When using Pipewine workflows, you have the following advantages:

- Pipewine can automatically draw a 2D representation of your workflow to help you (and others) understand what it does without reading your code. 
- Pipewine automatically attaches callbacks to all operations to track the progress of your workflow while it's running. Progress updates can be visualized live in a TUI (Text-based User Interface) to monitor the progress of long workflows.
- The code is transformed into a data structure that can be inspected before running it. 
- Pipewine can inject logic into your code (e.g. caches or checkpoints) without you having to write them manually.  

### Example Workflow

Here is an example of a workflow that does the following operations:

1. Read a "Letter" dataset - used many times as an example toy dataset in this tutorial. 
2. Repeat the dataset 10 times.
3. Apply random color jitter to each sample.
4. Group letters by their type (either 'vowel' or 'consonant').
5. Concatenate the two (vowels and consonants) splits into a single dataset.
6. Write the dataset (A).
7. Sort the vowels split by average color brightness.
8. Sort the consonants split by average color brightness.
9. Contatenate the sorted splits into a single dataset.
10. Write the dataset (B).

Isn't it much better to visualize this flow of operations as a 2D representation of a graph instead of as a 1D list of things to do? (This SVG was generated using Pipewine workflow drawer.)
![Workflow Example](../assets/wf.svg)

