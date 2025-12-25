from typing import Iterator, Dict, Any, List, Optional
import pandas as pd
from app.engine.transforms.base import BaseTransform
from app.core.errors import ConfigurationError

class JoinTransform(BaseTransform):
    """
    Joins multiple data streams horizontally.
    Config:
    - on: str (column name to join on)
    - how: str (left, right, inner, outer)
    """

    def validate_config(self) -> None:
        if "on" not in self.config:
            raise ConfigurationError("JoinTransform requires 'on' column.")

    def transform(self, data: Iterator[pd.DataFrame]) -> Iterator[pd.DataFrame]:
        raise NotImplementedError("JoinTransform requires multiple inputs. Use transform_multi instead.")

    def transform_multi(self, data_map: Dict[str, Iterator[pd.DataFrame]]) -> Iterator[pd.DataFrame]:
        join_on = self.config["on"]
        how = self.config.get("how", "left")
        
        # Identify Left and Right inputs
        # If we have exactly 2 inputs, arbitrary assignment if not specified
        # Ideal: Config should specify 'right_input_id'
        keys = list(data_map.keys())
        if len(keys) != 2:
            raise ConfigurationError(f"Join requires exactly 2 inputs, got {len(keys)}: {keys}")
        
        left_id, right_id = keys[0], keys[1]
        
        # Materialize Right Side (Lookup)
        # This is necessary for standard in-memory join.
        # Warning: High volume on right side will cause OOM.
        right_chunks = list(data_map[right_id])
        if not right_chunks:
            right_df = pd.DataFrame()
        else:
            right_df = pd.concat(right_chunks, ignore_index=True)
            
        # Stream Left Side
        left_iter = data_map[left_id]
        
        for df in left_iter:
            if join_on not in df.columns:
                yield df if how == "left" else pd.DataFrame()
                continue
            
            try:
                # Provide suffixes to avoid collision if column names overlap
                merged = pd.merge(df, right_df, on=join_on, how=how, suffixes=('_left', '_right'))
                yield merged
            except Exception as e:
                 raise ConfigurationError(f"Join operation failed: {e}")