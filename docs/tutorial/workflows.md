# ♻️ Workflows

More often than not, you will need to apply multiple operations to your data in a repeatable way: while it's true that this is exactly what a Python script is capable of doing, Pipewine has a concept called `Workflow` that does a few extra things for you.

With Pipewine, a `Workflow` is a Directed Acyclic Graph (DAG) where nodes are Actions (either a source, operator or sinks) and edges are data.