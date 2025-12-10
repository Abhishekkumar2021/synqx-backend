from typing import Iterator, Dict, Any, List, Optional
import pandas as pd
from app.engine.transforms.base import BaseTransform
from app.core.errors import ConfigurationError

class AggregateTransform(BaseTransform):
    """
    Groups data and calculates aggregates.
    Config:
    - group_by: List[str]
    - aggregates: Dict[str, str]  (e.g., {"salary": "mean", "id": "count"})
    """

    def validate_config(self) -> None:
        if "group_by" not in self.config:
            raise ConfigurationError("AggregateTransform requires 'group_by' in config.")
        if "aggregates" not in self.config:
            raise ConfigurationError("AggregateTransform requires 'aggregates' in config.")

    def transform(self, data: Iterator[pd.DataFrame]) -> Iterator[pd.DataFrame]:
        group_cols = self.config["group_by"]
        agg_map = self.config["aggregates"]
        
        # NOTE: Aggregation on streaming data (chunks) is complex because 
        # a group might be split across chunks. 
        # Ideally, this requires a stateful processor or a blocking operation 
        # that accumulates everything.
        # For this prototype, we will accumulate ALL chunks into memory (Warning: Memory Intensive)
        # then aggregate. A production system would use a database or specialized stream processor.
        
        accumulated_df = pd.DataFrame()
        
        for df in data:
            accumulated_df = pd.concat([accumulated_df, df])
            
        if not accumulated_df.empty:
            # Check if group columns exist
            valid_group_cols = [c for c in group_cols if c in accumulated_df.columns]
            if not valid_group_cols:
                 # If grouping keys are missing, we can't group. Return as is or empty? 
                 # Let's return empty to signify failure to group, or raise error.
                 # For now, just yield nothing.
                 return

            try:
                result = accumulated_df.groupby(valid_group_cols).agg(agg_map).reset_index()
                yield result
            except Exception as e:
                raise ConfigurationError(f"Aggregation failed: {e}")
