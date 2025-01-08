# Pipewine

Pipewine is a complete rewrite of the [Pipelime](https://github.com/eyecan-ai/pipelime-python.git) library and intends to serve the same purpose: provide a set of tools to manipulate multi-modal small/medium-sized datasets, mainly for research purposes in the CV/ML domain.

## 🚀 Features

- **Unified access pattern** to datasets of various formats, origin and content.
- **Underfolder**, a quick and easy filesystem-based dataset format good for small/medium datasets.
- **Common data encoding** formats for images, text and metadata.
- **Common operators** to manipulate existing datasets.
- **Workflows** that transform data in complex DAGs (Directed Acyclic Graph) pipelines.
- **CLI** (Command Line Interface) to quickly run simple workflows without writing a full python script.
- **Extendibility**, allowing the user to easily extend the library in many aspects, adding components that seamlessly integrate with the built-in ones:
    - Add custom dataset formats
    - Add custom data encoding formats
    - Add custom operators on datasets
    - Register components to the CLI

## ⭐️ Rationale

Pipewine started from a refactoring of some core components of the Pipelime library, but it soon turned into a complete rewrite that aims at solving some architectural issues of Pipelime, namely:

- **Dependency Hell**: Pipelime had so many of dependencies and contstraints that made it very hard to install in many environments due to conflicts and incompatibilities. Outdated dependencies at the core of the library (e.g. Pydantic 1) make it incompatible with new python packages that use Pydantic 2+. Upgrading these dependencies is currently impossible without breaking everything.
- **Over-reliance on Pydantic**: Making most library components inherit from Pydantic `BaseModel`'s (expecially with the 1.0 major) completely violate type-safety and result in type-checking errors if used with any modern python type-checker like MyPy or PyLance. As a result, IntelliSense and autocompletion do not work properly, requiring the user to fill their code with `type: ignore` directives. Furthermore, Pydantic runtime validation has some serious performance issues, slowing down the computation significantly (especially before rewriting the whole library in Rust).
- **Lack of typed abstractions**: Many components of Pipelime (e.g. `SamplesSequence`, `Sample`, `SampleStage`, `PipedSequence` and `PipelimeCommand`) do not carry any information on the type of data they operate on, thus making the usage of Pipelime error-prone: the user cannot rely on the type checker to know what type of data is currently processing. Some later components like `Entity` and `Action` aim at mitigating this problem by encapsulating samples inside a Pydantic model, but they ended up being almost unused because they integrate poorly with the rest of the library.
- **Confusing CLI**: Pipelime CLI was built with the purpose of being able to directly instantiate any (possibly nested) Pydantic model. This resulted in a very powerful CLI that could do pretty much anything but was also very confusing to use (and even worse to maintain). As a contributor of the pipelime library, I never met anybody able to read and understand the CLI validation errors.
- **Everything is CLI**: All Pipelime components are also CLI components. This has the positive effect of eliminating the need to manually write CLI hooks for data operators, but also couples the CLI with the API, requiring everything to be serializable as a list of key-value pairs of (possibly nested) builtin values. We solved this using Pydantic, but was it really worth the cost?
- **Feature Creep**: Lots of features like streams, remotes, choixe add a lot of complexity but are mostly unused. 

Key development decisions behind Pipewine:

- **Minimal Dependencies**: Rely on the standard library as much as possible, only depend on widely-used and maintained 3rd-party libraries. 
- **Type-Safety**: The library heavily relies on Python type annotations to achieve a desirable level of type safety at development-time. Runtime type checking is limited to external data validation to not hinder the performance too much. The user should be able to rely on any modern static type checker to notice and correct bugs. 
- **Pydantic**: Limit the use of Pydantic for stuff that is not strictly external data validation. When serialization and runtime validation are not needed, plain dataclasses are a perfect alternative.
- **CLI Segregation**: The CLI is merely a tool to quickly access some of the core library functionalities, no core component should ever depend on it. 
- **Limited Compatibility** Pipewine should be able to read data written by Pipelime and potentially be used alonside it, but it is not intended to be a backward-compatible update, it is in fact a separate project with a separate development cycle.
- **Feature Pruning** Avoid including complex features that no one is going to use, instead, focus on keeping the library easy to extend. 