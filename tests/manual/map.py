"""
N.B.: Not run by pytest.
"""
import time
from pathlib import Path
from pprint import pprint

from pydantic import BaseModel

from mppr.m_context import MContext

N_ROWS = 100


class Row(BaseModel):
    value: int


def square(key: str, row: Row) -> Row:
    time.sleep(0.1)
    return Row(value=row.value**2)


if __name__ == "__main__":
    data = MContext(Path("output")).init(
        stage_name="init",
        init_fn=lambda: {f"row{i}": Row(value=i) for i in range(N_ROWS)},
        to=Row,
    )
    data = data.map_resumable("square", square, to=Row)
    pprint(data.get())
