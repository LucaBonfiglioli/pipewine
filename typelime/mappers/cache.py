from typelime.mappers.base import Mapper
from typelime.sample import Sample
from typelime._op_typing import AnySample
from collections.abc import Sequence, Mapping
from typelime.bundle import Bundle
from typelime.item import MemoryItem


class CacheMapper[T: AnySample](Mapper[T, T]):
    def _apply_sample(self, sample: Sample) -> Sample:
        new_items = {}
        for k, v in sample.items():
            new_items[k] = MemoryItem(v(), v.get_parser())
        return sample.with_items(**new_items)

    def _apply_tuple(self, samples: tuple[Sample, ...]) -> tuple[Sample, ...]:
        return tuple(self._apply_sample(sample) for sample in samples)

    def _apply_sequence(self, samples: Sequence[Sample]) -> Sequence[Sample]:
        return [self._apply_sample(sample) for sample in samples]

    def _apply_mapping(self, samples: Mapping[str, Sample]) -> Mapping[str, Sample]:
        return {k: self._apply_sample(v) for k, v in samples.items()}

    def _apply_bundle(self, samples: Bundle[Sample]) -> Bundle[Sample]:
        return type(samples)(**self._apply_mapping(samples.as_dict()))

    def apply(self, x: T) -> T:
        if isinstance(x, Sample):
            return self._apply_sample(x)
        elif isinstance(x, tuple):
            return self._apply_tuple(x)
        elif isinstance(x, Sequence):
            return self._apply_sequence(x)
        elif isinstance(x, Mapping):
            return self._apply_mapping(x)
        else:
            return self._apply_bundle(x)
