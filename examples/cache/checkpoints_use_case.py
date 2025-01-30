from pathlib import Path
import shutil
import numpy as np
from pipewine import *
import tempfile


def extremely_expensive_api_that_costs_1_USD_per_image(prompt: str) -> np.ndarray:
    # 💸 cha-ching! 💸
    global balance
    balance -= 1

    # This is not actually a thing, we just return random noise.
    return np.random.randint(0, 255, (512, 512, 3), dtype=np.uint8)


class RegenerateImage(Mapper[Sample, Sample]):
    def __call__(self, idx: int, x: Sample) -> Sample:
        metadata = x["metadata"]()
        letter, color = metadata["letter"], metadata["color"]
        prompt = (
            f"Create an artistic representation of the letter {letter} in a visually "
            f"striking style. The letter should be prominently displayed in {color}, "
            "with a background that complements or contrasts it artistically. Use "
            "textures, lighting, and creative elements to make the design unique "
            "and aesthetically appealing."
        )
        gen_image = extremely_expensive_api_that_costs_1_USD_per_image(prompt)
        return x.with_value("image", gen_image)


if __name__ == "__main__":

    balance = 2500
    print(f"Initial balance: {balance}$")

    path = Path("tests/sample_data/underfolders/underfolder_0")
    dataset = UnderfolderSource(path)()
    dataset = MapOp(RegenerateImage())(dataset)

    # Comment the next 2 lines to go bankrupt!
    UnderfolderSink(ckpt := Path("/tmp/ckpt/"))(dataset)
    dataset = UnderfolderSource(ckpt)()

    dataset = RepeatOp(100)(dataset)

    for sample in dataset:
        ...

    print(f"Final balance: {balance}$")
