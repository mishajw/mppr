# mppr

A simple utility for mapping across lists of objects in Python with resumability.

Something that I've found myself reimplementing across several different projects, as well as being something that significantly speeds me up on experimental projects where I'm mapping over lots of data in expensive ways.

```python
from mppr import MContext

mcontext = MContext(Path("./mppr-cache"))
mdict = mcontext.init(
    "load_dataset",
    init_fn=load_prompts_from_huggingface,
    to=Prompt
)

mdict = mdict.map_resumable(
    "call_openai",
    fn=prompt_gpt4_turbo,
    to=Completion,
)

print(mdict.get())
# [
#   Completion(prompt="what's the capital of france?", completion="paris"),
#   Completion(prompt="what's the capital of spain?", completion="madrid"),
#   Completion(prompt="what's the capital of germany?", completion="berlin"),
#   ...
# ]
```

## Features

- Loading previously mapped data (`MContext.load`).
- Cached initializing of data (`MContext.init`).
- Resumable mapping (`MDict.map_resumable`).
- Async mapping (`MDict.amap_resumable`).
- Joining (`MDict.join`).
- Flat maps (`MDict.flat_map`).
- Filtering (`MDict.filter`).
- Sorting (`MDict.sort`).
- Converting to Pandas DataFrames (`MDict.to_dataframe`).
- Uploading data to S3 / specific file locations (`MDict.upload`).
- Downloading data from S3 / specific file locations (`MContext.download`).
- Support for Pydantic 2 base models (`to=YourPydanticBaseModel`).
- Support for pickle outputs (`to="pickle"`).
- `tqdm` for progress bars.
