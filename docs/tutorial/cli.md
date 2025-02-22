# 🖥️ CLI

Pipewine provides a command-line interface (CLI) that allows you to perform various operations on your data without writing any code. The CLI is built using [Typer](https://typer.tiangolo.com/), a library that makes it easy to create command-line interfaces in Python.

## Usage

To use the CLI, all you need to do is install Pipewine and run the `pipewine` command in your terminal. This will display a list of available commands and options.

```bash
pipewine
```

Equivalently, you can run the command with the `--help` flag to display the same information.

```bash
pipewine --help
```

The `--help` flag can also be used with any command to display information about that specific command.

```bash
pipewine command sub-command --help
```

To display the version of the current Pipewine installation, you can use the `--version` flag.

```bash
pipewine --version
```

### Mappers and Operators

To apply an operator to your data, you can use the `op` command followed by the operator name and the required arguments. You can get a full list of available operators using the `--help` flag with the `op` command.

```bash
pipewine op --help
```

Similarly, for mapper commands, you can use the `map` command followed by the mapper name and the required arguments. You can get a full list of available mappers using the `--help` flag with the `map` command.

```bash
pipewine map --help
```

Options differ between each operator, so you should use the `--help` flag with the operator name to get more information about the required arguments. E.g., to get more information about the `repeat` operator, you can run the following command:

```bash
pipewine op repeat --help
pipewine map hash --help
```

!!! example

    Suppose you want to repeat a dataset 10 times, you can do so using the `repeat` operator. The following command will repeat the dataset `my_dataset` 10 times and save the result to `my_repeated_dataset`:

    ```bash 
    pipewine op repeat -i my_dataset -o my_repeated_dataset --times 10
    ```

!!! success

    When you successfully run a command, you will see a message indicating that the operation was successful.

    ```bash
    ╭─ Workflow Status ────────────────╮
    │ Workflow completed successfully. │
    │ Started:  2025-02-22 11:39:38    │
    │ Finished: 2025-02-22 11:39:38    │
    │ Total duration: 0:00:00.207026   │
    ╰──────────────────────────────────╯
    ```

!!! failure

    If an error occurs during the execution of a command, you will see an error message indicating what went wrong.

    ```bash
    ╭─ Workflow Status ─────────────────────────────────────────────────────────────────────────────────────────────────╮
    │ Workflow failed.                                                                                                  │
    │                                                                                                                   │
    │ Traceback (most recent call last):                                                                                │
    │   File "/home/luca/repos/pipewine/pipewine/cli/utils.py", line 168, in run_cli_workflow                           │
    │     run_workflow(workflow, tracker=CursesTracker() if tui else None)                                              │
    │   File "/home/luca/repos/pipewine/pipewine/workflows/__init__.py", line 73, in run_workflow                       │
    │     raise e                                                                                                       │
    │   File "/home/luca/repos/pipewine/pipewine/workflows/__init__.py", line 70, in run_workflow                       │
    │     executor.execute(workflow)                                                                                    │
    │   File "/home/luca/repos/pipewine/pipewine/workflows/execution.py", line 273, in execute                          │
    │     self._execute_node(workflow, node, state, id_.hex, wf_opts)                                                   │
    │   File "/home/luca/repos/pipewine/pipewine/workflows/execution.py", line 167, in _execute_node                    │
    │     output = action(input_)                                                                                       │
    │              ^^^^^^^^^^^^^^                                                                                       │
    │   File "/home/luca/repos/pipewine/pipewine/sinks/underfolder.py", line 129, in __call__                           │
    │     raise FileExistsError(                                                                                        │
    │ FileExistsError: Folder /tmp/output already exists and policy OverwritePolicy.FORBID is used. Either change the   │
    │ destination path or set a weaker policy.                                                                          │
    │                                                                                                                   │
    │ Started:  2025-02-22 11:40:42                                                                                     │
    │ Finished: 2025-02-22 11:40:42                                                                                     │
    │ Total duration: 0:00:00.104007                                                                                    │
    ╰───────────────────────────────────────────────────────────────────────────────────────────────────────────────────╯

    ```

Both the `op` and `map` commands support changing the input and output formats using the `--input-format` (`-I`) and `--output-format` (`-O`) options. You can print the  
available input and output formats using the `--format-help` flag.

For convenience, this flag will also print all the recognized file extensions that can
be parsed by Pipewine items. 

```bash
pipewine op --format-help
pipewine map --format-help
```

By default, the input and output formats are set to `underfolder`. When using an underfolder as an output format, you can optionally specify, alongside the output path, the overwrite policy and copy policy to use. 

```
PATH[,OVERWRITE_POLICY[,COPY_POLICY]]
```

By default, the overwrite policy is set to `forbid` and the copy policy is set to `hard_link`.

!!! example

    Example, suppose we want to repeat a dataset 10 times and save the result to an underfolder named `my_repeated_dataset` that may already exist. We can do so using the following command:

    ```bash
    pipewine op repeat -i my_dataset -o my_repeated_dataset,overwrite --times 10
    ```

    Suppose we also want to change the copy policy and instead of hard linking the files, we want a full copy. We can do so using the following command:

    ```bash
    pipewine op repeat -i my_dataset -o my_repeated_dataset,overwrite,replicate --times 10
    ```

You can also use a custom `Grabber` to run the command using multi-processing parallelism. To do so, you can use the `--grabber` (`-g`) option followed by the number of processes to use and optionally the chunk size. 

!!! example

    ```bash
    pipewine op -g 8 repeat -i my_dataset -o my_repeated_dataset --times 100
    pipewine op -g 8,50 repeat -i my_dataset -o my_repeated_dataset --times 100
    ```

By default, commands report progress in a TUI interface. You can disable this behavior by using the `--no-tui` flag.

!!! example
    
    ```bash
    pipewine op --no-tui repeat -i my_dataset -o my_repeated_dataset --times 10
    ```

### Workflows

Pipewine also supports running workflows from the CLI. You can use the `wf` command followed by the workflow name and the required arguments. You can get a full list of available workflows using the `--help` flag with the `wf` command.

```bash
pipewine wf --help
```

Workflows can optionally be drawn using the `--draw [path]` option. This will disable the workflow execution and instead draw the workflow to the specified path. 

## Extension

Pipewine CLI is designed to be easily extensible, similarly to the old Pipelime CLI, by specifying a list of custom modules to load dynamically. These modules can be loaded using the `--module` (`-m`) option followed by the module name. 

Extension modules can be provided as a path to a Python file or a module path. 

!!! warning

    When loading an extension module from a python file, multiprocessing is disabled due to a limitation of the `multiprocessing` module.

    This issue was already present in the old Pipelime package and is due to the fact that the `multiprocessing` module fails to correctly modules that are imported dynamically from a single file, without a parent package.

    Multiprocessing should work correctly when loading modules from a package.

### Adding custom operators

To allow operators defined in python packages (or scripts) other than the `pipewine` package, all you need to do is create a function that creates the operator object decorate it with the `op_cli` decorator.

The decorated function is parsed by `Typer` and its signature is used to generate the CLI interface. You can use any type hints supported by `Typer` to specify the operator arguments, if any.

Do not worry about the input and output parameters, as `Pipewine` will automatically add them to the function signature dynamically, based on the type of operator returned by your function.

If, for some reason, you want to give a custom name to the command, you can use the `name` argument of the `op_cli` decorator.


!!! example

    This is how the `repeat` operator is defined in the `pipewine` package:

    ```python
    @op_cli()
    def repeat(
        times: Annotated[int, Option(..., "--times", "-t")],
        interleave: Annotated[bool, Option(..., "--interleave", "-I")] = False,
    ) -> RepeatOp:
        """Repeat a dataset N times replicating the samples."""
        return RepeatOp(times, interleave=interleave)
    ```

    Equivalently, if you don't like the `Annotated` syntax you can use the old (deprecated) `Option` default value syntax:

    ```python
    @op_cli()
    def repeat(
        times: int = Option(..., "--times", "-t"),
        interleave: bool = Option(False, "--interleave", "-I"),
    ) -> RepeatOp:
        """Repeat a dataset N times replicating the samples."""
        return RepeatOp(times, interleave=interleave)
    ```

    If you are rapid-protyping and don't want to write the help strings, you can avoid
    using `Option` altogether and just use the default values:

    ```python
    @op_cli()
    def repeat(times: int, interleave: bool = False) -> RepeatOp:
        return RepeatOp(times, interleave=interleave)
    ```

!!! note

    It is crucial that the function return type is correctly annotated with the type of operator that it returns. This is used by `Pipewine` to correctly parse the operator and its arguments, and to generate the correct CLI interface.

If the command has some eager behavior that can be parallelized using a `Grabber`, you can add a `grabber: Grabber` positional argument to the function signature. Pipewine will automatically recognize this argument and pass a `Grabber` object to the function when it is called.

!!! Example

    This is how the `sort` operator, which has some eager computation that can be parallelized, is defined in the `pipewine` package:

    ```python
    @op_cli()
    def sort(
        grabber: Grabber,
        key: Annotated[str, Option(..., "--key", "-k")],
        reverse: Annotated[bool, Option(..., "--reverse", "-r")] = False,
    ) -> SortOp:
        ... # omitted for brevity
    ```


### Adding custom mappers

Adding custom mappers is very similar to adding custom operators. The only difference is that you need to use the `map_cli` decorator instead of the `op_cli` decorator, and of course, the function should return a `Mapper` object instead of an `Operator` object.

Everything else is the same, including the possibility to specify a custom name for the command using the `name` argument of the `map_cli` decorator.

!!! example

    This is how the `filter-keys` mapper is defined in the `pipewine` package:

    ```python
    @map_cli()
    def filter_keys(
        keys: Annotated[list[str], Option(..., "-k", "--keys")],
        negate: Annotated[bool, Option(..., "-n", "--negate")] = False,
    ) -> FilterKeysMapper:
        """Keep only or remove a subset of items."""
        return FilterKeysMapper(keys, negate=negate)
    ```

### Adding custom data formats

Currently, the Pipewine CLI only supports the `underfolder` format for both input and output. However, you can easily register custom `DatasetSource` and `DatasetSink` classes to support additional data formats.

This can be done using the `source_cli` and `sink_cli` decorators, respectively. 

To add a custom input format, you can use the `source_cli` decorator on a function that 
accepts:

- A `str` argument containing some text that must be parsed into a `DatasetSource` object.
- A `Grabber` argument that may be used by the dataset source, in case it requires to do some eager computation that can run in parallel.
- A `type[Sample]` argument with the concrete type of the samples that the dataset source will return. 

To add a custom output format, you can use the `sink_cli` decorator on a function that
accepts:

- A `str` argument containing some text that must be parsed into a `DatasetSink` object.
- A `Grabber` argument that may be used by the dataset sink, in case it can parallelize the writing of the dataset.

(The sample type is not needed for the output format, as the dataset sink can simply infer it from the samples that it receives.)

You can use the docstring of the function to specify the help message that will be displayed with the `--format-help` flag.

You can also specify a custom name for the input/output using the `name` argument of the `source_cli` or `sink_cli` decorator.

!!! note

    Input and output formats are not Typer commands since they cannot be called directly from the CLI. Instead, they are only used to parse the input/output text according to the format specified by the user.

    Hence, the function signature should not include typer-specific annotations, such as `Option` or `Argument`.

!!! example

    This is how the `underfolder` format is registered to the CLI:

    ```python
    @source_cli()
    def underfolder(
        text: str, grabber: Grabber, sample_type: type[Sample]
    ) -> UnderfolderSource:
        """PATH: Path to the dataset folder."""
        return UnderfolderSource(Path(text), sample_type=sample_type)

    @sink_cli()
    def underfolder(text: str, grabber: Grabber) -> UnderfolderSink:
        """PATH[,OVERWRITE=forbid[,COPY_POLICY=hard_link]]"""
        path, ow_policy, copy_policy = _split_and_parse_underfolder_text(text)
        return UnderfolderSink(
            Path(path), grabber=grabber, overwrite_policy=ow_policy, copy_policy=copy_policy
        )
    ```

### Adding custom workflows

Adding custom workflows is very similar to adding custom operators and mappers, with the following differences:

- You need to use the `wf_cli` decorator instead.
- The function must return a `Workflow` object.
- Pipewine will not automatically add the input and output parameters to the function, so you need to specify them explicitly in the function signature.

!!! example

    Example workflow:

    ```python
    @wf_cli(name="example")
    def example(
        input: Annotated[Path, Option(..., "-i", "--input", help="Input folder.")],
        output: Annotated[Path, Option(..., "-o", "--output", help="Output folder.")],
        repeat_n: Annotated[
            int, Option(..., "-r", "--repeat", help="Repeat n times.")
        ] = 100,
        workers: Annotated[int, Option(..., "-w", "--workers", help="Num workers.")] = 0,
    ) -> Workflow:
        grabber = Grabber(workers, 50)
        wf = Workflow(WfOptions(checkpoint_grabber=grabber))
        data = wf.node(UnderfolderSource(input, sample_type=LetterSample))()
        data = wf.node(RepeatOp(repeat_n))(data)
        data = wf.node(MapOp(ColorJitter()), options=WfOptions(checkpoint=True))(data)
        groups = wf.node(GroupByOp(group_fn, grabber=None))(data)
        vowels = groups["vowel"]
        consonants = groups["consonant"]
        vowels = wf.node(SortOp(sort_fn, grabber=None))(vowels)
        consonants = wf.node(SortOp(sort_fn, grabber=None))(consonants)
        data = wf.node(CatOp())([vowels, consonants])
        wf.node(UnderfolderSink(output, grabber=grabber))(data)
        return wf
    ```