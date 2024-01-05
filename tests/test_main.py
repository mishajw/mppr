from pathlib import Path
from tempfile import TemporaryDirectory

from pydantic import BaseModel
from mppr import Mappable


class Row(BaseModel):
    value: int


def test_simple():
    with TemporaryDirectory() as tmp_dir:
        values = Mappable(
            {
                "row1": Row(value=1),
                "row2": Row(value=2),
                "row3": Row(value=3),
            },
            base_dir=Path(tmp_dir) / "output",
        )
        values = values.map(
            "increment",
            lambda _, row: Row(value=row.value + 1),
            clazz=Row,
        )
        values = values.map(
            "sqaure",
            lambda _, row: Row(value=row.value**2),
            clazz=Row,
        )

    assert values.get() == [Row(value=4), Row(value=9), Row(value=16)]


def test_resume():
    with TemporaryDirectory() as tmp_dir:
        values = Mappable(
            {
                "row1": Row(value=1),
                "row2": Row(value=2),
                "row3": Row(value=3),
            },
            base_dir=Path(tmp_dir) / "output",
        )
        values = values.map(
            "increment",
            lambda _, row: Row(value=row.value + 1),
            clazz=Row,
        )

        def throw_lambda(key: str, row: Row) -> Row:
            raise Exception("This should not be called")

        values = values.map(
            "increment",
            throw_lambda,
            clazz=Row,
        )

    assert values.get() == [Row(value=2), Row(value=3), Row(value=4)]
