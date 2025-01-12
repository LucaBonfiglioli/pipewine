from pathlib import Path

from pipewine import TypelessSample, UnderfolderSource

if __name__ == "__main__":
    # Create the source object from an existing directory Path.
    path = Path("tests/sample_data/underfolders/underfolder_0")
    source: UnderfolderSource[TypelessSample] = UnderfolderSource(path)

    # Call the source object to create a new Dataset instance.
    dataset = source()

    # Do stuff with the dataset
    sample = dataset[4]
    print(sample["image"]().reshape(-1, 3).mean(0))  # >>> [244.4, 231.4, 221.7]
    print(sample["metadata"]()["color"])  #            >>> "orange"
    print(sample["shared"]()["vowels"])  #             >>> ["a", "e", "i", "o", "u"]
