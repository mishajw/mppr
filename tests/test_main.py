from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any

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

        values = values.map(
            "increment",
            _throw_lambda,
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


def test_pickle_resume():
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
            "increment",
            _throw_lambda,
            to="pickle",
        )

    assert values.get() == [dict(value=2), dict(value=3), dict(value=4)]


def test_limit():
    with TemporaryDirectory() as tmp_dir:
        values = Mappable(
            {
                "row1": dict(value=1),
                "row2": dict(value=2),
                "row3": dict(value=3),
            },
            base_dir=Path(tmp_dir) / "output",
        )
        values = values.limit(2)

    assert values.get() == [dict(value=1), dict(value=2)]


def test_limit_change():
    with TemporaryDirectory() as tmp_dir:
        values = Mappable(
            {
                "row1": Row(value=1),
                "row2": Row(value=2),
                "row3": Row(value=3),
            },
            base_dir=Path(tmp_dir) / "output",
        )
        values.map(
            "increment",
            lambda _, row: Row(value=row.value + 1),
            to="pickle",
        )

        values_incremented = values.limit(2).map(
            "increment",
            lambda _, row: Row(value=row.value + 1),
            to="pickle",
        )

    assert values_incremented.get() == [Row(value=2), Row(value=3)]


def test_join():
    with TemporaryDirectory() as tmp_dir:
        v1 = Mappable(
            {
                "row1": Row(value=1),
                "row2": Row(value=2),
                "row3": Row(value=3),
            },
            base_dir=Path(tmp_dir) / "output",
        )
        v2 = v1.map(
            "increment",
            lambda _, row: Row(value=row.value + 1),
            to=Row,
        )

        v3 = v1.join(v2, lambda _, r1, r2: Row(value=r1.value + r2.value))

    assert v3.get() == [Row(value=3), Row(value=5), Row(value=7)]


def test_to_dataframe():
    with TemporaryDirectory() as tmp_dir:
        values = Mappable(
            {
                "row1": Row(value=1),
                "row2": Row(value=2),
                "row3": Row(value=3),
            },
            base_dir=Path(tmp_dir) / "output",
        )
        df = values.to_dataframe(lambda row: row.model_dump())

    assert df.index.tolist() == ["row1", "row2", "row3"]
    assert df.loc["row1"]["value"] == 1
    assert df.loc["row2"]["value"] == 2
    assert df.loc["row3"]["value"] == 3


def test_flat_map():
    with TemporaryDirectory() as tmp_dir:
        values = Mappable(
            {
                "row1": Row(value=1),
                "row2": Row(value=2),
                "row3": Row(value=3),
            },
            base_dir=Path(tmp_dir) / "output",
        )
        values = values.flat_map(
            lambda key, row: {f"{key}_pos": row, f"{key}_neg": Row(value=-row.value)}
        )

    assert values.get() == [
        Row(value=1),
        Row(value=-1),
        Row(value=2),
        Row(value=-2),
        Row(value=3),
        Row(value=-3),
    ]


def test_upload():
    with TemporaryDirectory() as tmp_dir:
        values = Mappable(
            {
                "row1": Row(value=1),
                "row2": Row(value=2),
                "row3": Row(value=3),
            },
            base_dir=Path(tmp_dir) / "output",
        )
        values.upload(
            f"{tmp_dir}/upload-test",
            to=Row,
        )
        assert (Path(tmp_dir) / "upload-test.jsonl").is_file()
        assert (Path(tmp_dir) / "upload-test.jsonl").read_text() == (
            '{"key": "row1", "value": {"value": 1}}\n'
            '{"key": "row2", "value": {"value": 2}}\n'
            '{"key": "row3", "value": {"value": 3}}\n'
        )


def test_filter():
    with TemporaryDirectory() as tmp_dir:
        values = Mappable(
            {
                "row1": Row(value=1),
                "row2": Row(value=2),
                "row3": Row(value=3),
            },
            base_dir=Path(tmp_dir) / "output",
        )
        values = values.filter(
            lambda _, row: row.value % 2 == 1,
        )

    assert values.get() == [Row(value=1), Row(value=3)]


def test_sort():
    with TemporaryDirectory() as tmp_dir:
        values = Mappable(
            {
                "row1": Row(value=2),
                "row2": Row(value=1),
                "row3": Row(value=3),
            },
            base_dir=Path(tmp_dir) / "output",
        )
        assert values.get() == [Row(value=2), Row(value=1), Row(value=3)]
        values = values.sort(lambda _, row: row.value)
        assert values.get() == [Row(value=1), Row(value=2), Row(value=3)]


def _throw_lambda(key: str, row: Any) -> Row:
    raise Exception("This should not be called")
