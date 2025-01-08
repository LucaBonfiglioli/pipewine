from pathlib import Path

from pipewine import UnderfolderSource, UnderfolderSink
from tempfile import gettempdir

if __name__ == "__main__":
    # Read a dataset with an underfolder source
    input_path = Path("tests/sample_data/underfolders/underfolder_0")
    source = UnderfolderSource(input_path)
    dataset = source()

    # Write the dataset with an underfolder sink
    output_path = Path(gettempdir()) / "underfolder_write_example"
    sink = UnderfolderSink(output_path)
    sink(dataset)
