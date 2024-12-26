# [WIP] 🚧 Pipewine 🚧 [WIP] 
 
> [!WARNING] 
> This project is **under construction** and **not** ready for use yet.
> 
> Currently missing:
> - Many core features
> - Many secondary features
> - Proper testing
> - Documentation

Pipewine is a complete rewrite of the [Pipelime](https://github.com/eyecan-ai/pipelime-python.git) library: a set of tools to manipulate multi-modal small and medium-sized datasets, intended mainly for research purposes in the CV/ML domain.

This rewrite aims at solving some major architectural issues of Pipelime, namely:
- 💥 Large set of dependencies and constraints that cause installation conflicts.
- 🧓 Outdated dependencies at the core of the library (e.g. Pydantic 1.x) make it incompatible with newer environments. Upgrading to a newer version is currently impossible without breaking everything.
- ❌ Making most library components inherit from Pydantic `BaseModel`'s completely violate type-safety and result in type-checking errors if used with any modern python type-checker like MyPy or PyLance. As a result:
  -  IntelliSense and autocompletion do not work properly.
  -  User's type-checking errors mix with errors resulting from the library generalized type-unsafety, making it difficult for the user to notice errors on their side.
  -  Filling the code with `type: ignore` directives is distracting and results in worse code readability.
- 🐌 Furthermore, Pydantic runtime validation has some serious performance issues, slowing down the computation significantly.
- 🤬 Lack of typed abstractions for `SamplesSequence`, `Sample`, `SampleStage`, `PipedSequence` and `PipelimeCommand` make the usage of Pipelime error-prone. `Entity` and `Action` integrate poorly with the rest of the library.
- 😕 Old CLI is very powerful but also confusing to use and to maintain. 
- 💀 Lots of features like streams, remotes, choixe add complexity but are mostly unused. 

Development rationale behind Pipewine:

- Rely on the standard library as much as possible. 
- The library should be installable in most python environments.
- Renounce Pydantic, and all its works, and all its temptations.
- A type error should always imply user error.
- The user should be able to rely on the intellisense before having to resort the documentation (or even reading the library code). 
- The user shouldn't feel the need to disable the type-checker.
- Pipewine should not interfere too much with the user code. 
- Pipewine should be able to read data written by Pipelime.
- Avoid including complex features that no one is going to use. 