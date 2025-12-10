from typing import Iterator, Dict, Any, List, Optional
import pandas as pd
from app.engine.transforms.base import BaseTransform
from app.core.errors import ConfigurationError

class FilterTransform(BaseTransform):
    """
    Filters rows based on a query string.
    Config:
    - condition: str (e.g., "age > 30 and status == 'active'")
    """

    def validate_config(self) -> None:
        if "condition" not in self.config:
            raise ConfigurationError("FilterTransform requires 'condition' in config.")

    def transform(self, data: Iterator[pd.DataFrame]) -> Iterator[pd.DataFrame]:
        condition = self.config["condition"]
        for df in data:
            try:
                # Pandas query method is relatively safe for simple expressions
                yield df.query(condition)
            except Exception as e:
                raise ConfigurationError(f"Filter failed with condition '{condition}': {e}")
