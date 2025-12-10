from typing import Iterator, Dict, Any, List, Optional
import pandas as pd
from app.engine.transforms.base import BaseTransform
from app.core.errors import ConfigurationError

class DeduplicateTransform(BaseTransform):
    """
    Removes duplicate rows from the DataFrame.
    Config:
    - subset: Optional[List[str]] (Columns to consider for identifying duplicates, default all)
    - keep: str (Which duplicate to keep: 'first', 'last', or False to drop all duplicates)
    """

    def validate_config(self) -> None:
        if "keep" in self.config and self.config["keep"] not in ["first", "last", False]:
            raise ConfigurationError("DeduplicateTransform 'keep' must be 'first', 'last', or False.")
        if "subset" in self.config and not isinstance(self.config["subset"], list):
             raise ConfigurationError("DeduplicateTransform 'subset' must be a list of column names.")

    def transform(self, data: Iterator[pd.DataFrame]) -> Iterator[pd.DataFrame]:
        subset = self.config.get("subset")
        keep = self.config.get("keep", "first") # Default 'first' if not specified

        for df in data:
            # Filter out subset columns that don't exist in the current DataFrame
            valid_subset = [col for col in subset if col in df.columns] if subset else None
            
            if valid_subset is None and subset is not None: # If subset was specified but none are valid
                yield df # No valid columns to deduplicate on, return original
            else:
                yield df.drop_duplicates(subset=valid_subset, keep=keep)
