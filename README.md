# mppr

A simple utility for mapping across lists of objects in Python with resumability.

Something that I've found myself reimplementing across several different projects, as well as being something that significantly speeds me up on experimental projects where I'm mapping over lots of data in expensive ways.

```python
from pydantic import BaseModel
class Prompt(BaseModel):
    text: str
class Completion(BaseModel):
    prompt: str
    completion: str

from mppr import mppr
data = mppr.init(
    "load_dataset",
    base_dir=Path("./mppr-cache"),
    init_fn=load_prompts_from_huggingface,
    clazz=Prompt
)
data = data.map(
    "call_openai",
    fn=prompt_gpt4_turbo,
    clazz=Completion,
)
print(data.get())
# [
#   Completion(prompt="what's the capital of france?", completion="paris"),
#   Completion(prompt="what's the capital of spain?", completion="madrid"),
#   Completion(prompt="what's the capital of germany?", completion="berlin"),
#   ...
# ]
```
