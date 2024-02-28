from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any, Generator

import pytest
from pydantic import BaseModel

from mppr import MContext


class Row(BaseModel):
    value: int


@pytest.fixture
def temp_dir() -> Generator[Path, None, None]:
    with TemporaryDirectory() as tmp_dir:
        yield Path(tmp_dir)


@pytest.fixture
def mcontext(temp_dir: Path) -> MContext:
    return MContext(dir=temp_dir)


def test_simple(mcontext: MContext):
    values = mcontext.create(
        {
            "row1": Row(value=1),
            "row2": Row(value=2),
            "row3": Row(value=3),
        },
    )
    values = values.map_cached(
        "increment",
        lambda _, row: Row(value=row.value + 1),
        to=Row,
    )
    values = values.map_cached(
        "sqaure",
        lambda _, row: Row(value=row.value**2),
        to=Row,
    )

    assert values.get() == [Row(value=4), Row(value=9), Row(value=16)]


@pytest.mark.asyncio
async def test_async(mcontext: MContext):
    values = mcontext.create(
        {
            "row1": Row(value=1),
            "row2": Row(value=2),
            "row3": Row(value=3),
        },
    )

    async def increment(key: str, row: Row) -> Row:
        return Row(value=row.value + 1)

    values = await values.amap_cached(
        "increment",
        fn=increment,
        to=Row,
    )

    assert values.get() == [Row(value=2), Row(value=3), Row(value=4)]


def test_map(mcontext: MContext):
    values = mcontext.create(
        {
            "row1": Row(value=1),
            "row2": Row(value=2),
            "row3": Row(value=3),
        },
    )
    values = values.map(
        lambda key, row: (key, row.value + 1),
    )

    assert values.get() == [("row1", 2), ("row2", 3), ("row3", 4)]


def test_resume(mcontext: MContext):
    values = mcontext.create(
        {
            "row1": Row(value=1),
            "row2": Row(value=2),
            "row3": Row(value=3),
        },
    )
    values = values.map_cached(
        "increment",
        lambda _, row: Row(value=row.value + 1),
        to=Row,
    )

    values = values.map_cached(
        "increment",
        _throw_lambda,
        to=Row,
    )

    assert values.get() == [Row(value=2), Row(value=3), Row(value=4)]


def test_pickle(mcontext: MContext):
    values = mcontext.create(
        {
            "row1": dict(value=1),
            "row2": dict(value=2),
            "row3": dict(value=3),
        },
    )
    values = values.map_cached(
        "increment",
        lambda _, row: dict(value=row["value"] + 1),
        to="pickle",
    )
    values = values.map_cached(
        "sqaure",
        lambda _, row: dict(value=row["value"] ** 2),
        to="pickle",
    )

    assert values.get() == [dict(value=4), dict(value=9), dict(value=16)]


def test_pickle_resume(mcontext: MContext):
    values = mcontext.create(
        {
            "row1": dict(value=1),
            "row2": dict(value=2),
            "row3": dict(value=3),
        },
    )
    values = values.map_cached(
        "increment",
        lambda _, row: dict(value=row["value"] + 1),
        to="pickle",
    )

    values = values.map_cached(
        "increment",
        _throw_lambda,
        to="pickle",
    )

    assert values.get() == [dict(value=2), dict(value=3), dict(value=4)]


def test_limit(mcontext: MContext):
    values = mcontext.create(
        {
            "row1": dict(value=1),
            "row2": dict(value=2),
            "row3": dict(value=3),
        },
    )
    values = values.limit(2)

    assert values.get() == [dict(value=1), dict(value=2)]


def test_limit_change(mcontext: MContext):
    values = mcontext.create(
        {
            "row1": Row(value=1),
            "row2": Row(value=2),
            "row3": Row(value=3),
        },
    )
    values.map_cached(
        "increment",
        lambda _, row: Row(value=row.value + 1),
        to="pickle",
    )

    values_incremented = values.limit(2).map_cached(
        "increment",
        lambda _, row: Row(value=row.value + 1),
        to="pickle",
    )

    assert values_incremented.get() == [Row(value=2), Row(value=3)]


def test_join(mcontext: MContext):
    v1 = mcontext.create(
        {
            "row1": Row(value=1),
            "row2": Row(value=2),
            "row3": Row(value=3),
        },
    )
    v2 = v1.map_cached(
        "increment",
        lambda _, row: Row(value=row.value + 1),
        to=Row,
    )

    v3 = v1.join(v2, lambda _, r1, r2: Row(value=r1.value + r2.value))

    assert v3.get() == [Row(value=3), Row(value=5), Row(value=7)]


def test_to_dataframe(mcontext: MContext):
    values = mcontext.create(
        {
            "row1": Row(value=1),
            "row2": Row(value=2),
            "row3": Row(value=3),
        },
    )
    df = values.to_dataframe(lambda row: row.model_dump())

    assert df.index.tolist() == ["row1", "row2", "row3"]
    assert df.loc["row1"]["value"] == 1
    assert df.loc["row2"]["value"] == 2
    assert df.loc["row3"]["value"] == 3


def test_flat_map(mcontext: MContext):
    values = mcontext.create(
        {
            "row1": Row(value=1),
            "row2": Row(value=2),
            "row3": Row(value=3),
        },
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


def test_upload(mcontext: MContext, temp_dir: Path):
    values = mcontext.create(
        {
            "row1": Row(value=1),
            "row2": Row(value=2),
            "row3": Row(value=3),
        },
    )
    values.upload(
        f"{temp_dir}/upload-test",
        to=Row,
    )
    assert (temp_dir / "upload-test.jsonl").is_file()
    assert (temp_dir / "upload-test.jsonl").read_text() == (
        '{"key": "row1", "value": {"value": 1}}\n'
        '{"key": "row2", "value": {"value": 2}}\n'
        '{"key": "row3", "value": {"value": 3}}\n'
    )


def test_filter(mcontext: MContext):
    values = mcontext.create(
        {
            "row1": Row(value=1),
            "row2": Row(value=2),
            "row3": Row(value=3),
        },
    )
    values = values.filter(
        lambda _, row: row.value % 2 == 1,
    )

    assert values.get() == [Row(value=1), Row(value=3)]


def test_sort(mcontext: MContext):
    values = mcontext.create(
        {
            "row1": Row(value=2),
            "row2": Row(value=1),
            "row3": Row(value=3),
        },
    )
    assert values.get() == [Row(value=2), Row(value=1), Row(value=3)]
    values = values.sort(lambda _, row: row.value)
    assert values.get() == [Row(value=1), Row(value=2), Row(value=3)]


def test_rekey(mcontext: MContext):
    values = mcontext.create(
        {
            "row1": Row(value=1),
            "row2": Row(value=2),
            "row3": Row(value=3),
        },
    )
    values = values.rekey(lambda key, _: f"{key}_new")

    assert values.get_keys() == ["row1_new", "row2_new", "row3_new"]


def test_rekey_and_group(mcontext: MContext):
    values = mcontext.create(
        {
            "row1": Row(value=1),
            "row2": Row(value=2),
            "row3": Row(value=3),
            "row4": Row(value=1),
        },
    )
    values = values.rekey_and_group(lambda _, __: "new")

    assert values.get() == [[Row(value=1), Row(value=2), Row(value=3), Row(value=1)]]


def _throw_lambda(key: str, row: Any) -> Row:
    raise Exception("This should not be called")
