# 🛠️ Installation

## Basic Installation

Before installing make sure you have:

- A Python3.12+ interpreter installed on your system.
- A [Virtual Environment](https://docs.python.org/3/library/venv.html), which is highly recommended to avoid messing up your system-level python environment.
- A relatively up-to-date version of `pip`. You can upgrade to the latest version using
  
    ```bash
    pip install -U pip
    ```

To install **Pipewine**, run:

```bash
pip install pipewine
```

You can then verify whether `pipewine` was correctly installed by calling the CLI:

```bash
pipewine --version
```

!!! success

    In case the installation is successful, you should see the version of the current Pipewine installation, e.g:

    ```
    0.1.0
    ```

!!! failure

    In case something went wrong, you should see something like (this may vary based on your shell type):

    ```
    bash: command not found: pipewine
    ```

    In this case, do the following:

    1. Check for any `pip` error messages during installation.
    2. Go back through all steps and check whether you followed them correctly.
    3. Open a [GitHub issue](https://github.com/lucabonfiglioli/pipewine/issues) describing the installation problem. 

## Dev Installation

If you are a dev and want to install Pipewine for development purposes, it's recommended you follow these steps instead:

1. Clone the github repo in a folder of your choice:
    ```bash
    git clone https://github.com/lucabonfiglioli/pipewine.git
    cd pipewine
    ```
2. Create a new virtual environment:
    ```bash
    python3.12 -m venv .venv
    source .venv/bin/activate 
    ```
3. Update pip:
    ```bash
    pip install -U pip
    ```
4. Install pipewine in edit mode:
    ```bash
    pip install -e .
    ```
5. Install pipewine optional dependencies
    ```bash
    pip install .[dev] .[docs]
    ```

    !!! warning

        With some shells (like `zsh`), you may need to escape the square brackets e.g: `.\[dev\]` or `.\[docs\]`.
