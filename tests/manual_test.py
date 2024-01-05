"""
N.B.: Not run by pytest.
"""
import time
from pathlib import Path
from pprint import pprint

from pydantic import BaseModel

from mppr import mppr

N_ROWS = 100


class Row(BaseModel):
    value: int


def square(key: str, row: Row) -> Row:
    time.sleep(0.1)
    return Row(value=row.value**2)


data = mppr.init(
    stage_name="init",
    base_dir=Path("output"),
    init_fn=lambda: {f"row{i}": Row(value=i) for i in range(N_ROWS)},
    clazz=Row,
)
data = data.map("square", square, clazz=Row)
pprint(data.get())
