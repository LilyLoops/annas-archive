from allthethings.openlibrary_marc.marc_base import MarcBase, MarcFieldBase
from collections.abc import Iterator

class DataField(MarcFieldBase):
    def __init__(self, rec, json) -> None:
        self.rec = rec
        self.json = json

    def ind1(self) -> str:
        return self.json['ind1']

    def ind2(self) -> str:
        return self.json['ind2']

    def get_all_subfields(self) -> Iterator[tuple[str, str]]:
        for subfield in self.json['subfields']:
            for k, v in subfield.items():
                yield k, v


class MarcJson(MarcBase):
    def __init__(self, json) -> None:
        self.json = json

    def read_fields(self, want: list[str]) -> Iterator[tuple[str, str | DataField]]:
        for field in self.json['fields']:
            for k, v in field.items():
                if k not in want:
                    continue
                if type(v) is str:
                    yield k, v
                else:
                    yield k, DataField(self, v)
