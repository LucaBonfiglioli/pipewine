# Pipewine

<p align="center">
<a href='https://coveralls.io/github/LucaBonfiglioli/pipewine?branch=develop'><img src='https://coveralls.io/repos/github/LucaBonfiglioli/pipewine/badge.svg?branch=develop' alt='Coverage Status' /></a>
<a href='https://pypi.org/project/pipewine/'><img src='https://img.shields.io/pypi/v/pipewine' alt='PyPI version' /></a>
<a href='https://lucabonfiglioli.github.io/pipewine'><img src='https://img.shields.io/badge/docs-latest-blue' alt='Documentation' /></a>
<a href='https://unlicense.org/'><img src='https://img.shields.io/badge/license-Unlicense-blue' alt='License' /></a>
</p>

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

## Documentation

Full documentation is available at [lucabonfiglioli.github.io/pipewine](https://lucabonfiglioli.github.io/pipewine).

## Installation

Pipewine is available on PyPI and can be installed via pip:

```bash
pip install pipewine
```

## License

Pipewine is public domain software released under the [Unlicense](https://unlicense.org/). You can do whatever you want with it.

