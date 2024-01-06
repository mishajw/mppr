from pathlib import Path
from tempfile import TemporaryDirectory

import pytest
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
            to=Row,
        )
        values = values.map(
            "sqaure",
            lambda _, row: Row(value=row.value**2),
            to=Row,
        )

    assert values.get() == [Row(value=4), Row(value=9), Row(value=16)]


@pytest.mark.asyncio
async def test_async():
    with TemporaryDirectory() as tmp_dir:
        values = Mappable(
            {
                "row1": Row(value=1),
                "row2": Row(value=2),
                "row3": Row(value=3),
            },
            base_dir=Path(tmp_dir) / "output",
        )

        async def increment(key: str, row: Row) -> Row:
            return Row(value=row.value + 1)

        values = await values.amap(
            "increment",
            fn=increment,
            to=Row,
        )

    assert values.get() == [Row(value=2), Row(value=3), Row(value=4)]


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
            to=Row,
        )

        def throw_lambda(key: str, row: Row) -> Row:
            raise Exception("This should not be called")

        values = values.map(
            "increment",
            throw_lambda,
            to=Row,
        )

    assert values.get() == [Row(value=2), Row(value=3), Row(value=4)]


def test_pickle():
    with TemporaryDirectory() as tmp_dir:
        values = Mappable(
            {
                "row1": dict(value=1),
                "row2": dict(value=2),
                "row3": dict(value=3),
            },
            base_dir=Path(tmp_dir) / "output",
        )
        values = values.map(
            "increment",
            lambda _, row: dict(value=row["value"] + 1),
            to="pickle",
        )
        values = values.map(
            "sqaure",
            lambda _, row: dict(value=row["value"] ** 2),
            to="pickle",
        )

    assert values.get() == [dict(value=4), dict(value=9), dict(value=16)]
