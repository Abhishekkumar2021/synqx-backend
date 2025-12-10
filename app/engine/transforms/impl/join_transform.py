from typing import Iterator, Dict, Any, List, Optional
import pandas as pd
from app.engine.transforms.base import BaseTransform
from app.core.errors import ConfigurationError

class JoinTransform(BaseTransform):
    """
    Joins the current stream with a static dataset or another stream (simulated here).
    For this MVP, we will simulate joining with a 'lookup' table defined in config 
    or loaded from a file path provided in config.
    
    Config:
    - on: str (column name to join on)
    - how: str (left, right, inner, outer)
    - lookup_data: List[Dict] (static data to join with) OR
    - lookup_file: str (path to CSV/JSON to load as lookup)
    """

    def validate_config(self) -> None:
        if "on" not in self.config:
            raise ConfigurationError("JoinTransform requires 'on' column.")
        if "lookup_data" not in self.config and "lookup_file" not in self.config:
            raise ConfigurationError("JoinTransform requires 'lookup_data' or 'lookup_file'.")

    def transform(self, data: Iterator[pd.DataFrame]) -> Iterator[pd.DataFrame]:
        join_on = self.config["on"]
        how = self.config.get("how", "left")
        
        # Load lookup data once
        lookup_df = pd.DataFrame()
        if "lookup_data" in self.config:
            lookup_df = pd.DataFrame(self.config["lookup_data"])
        elif "lookup_file" in self.config:
            try:
                # Basic file loading support
                fp = self.config["lookup_file"]
                if fp.endswith(".csv"):
                    lookup_df = pd.read_csv(fp)
                elif fp.endswith(".json"):
                    lookup_df = pd.read_json(fp)
            except Exception as e:
                raise ConfigurationError(f"Failed to load lookup file: {e}")

        if lookup_df.empty:
            # Nothing to join, just yield original or empty?
            # Yielding original implies failed join (if inner) or passed through (if left)
            # Let's yield original if left/outer, else empty
            for df in data:
                 yield df if how in ["left", "outer"] else pd.DataFrame()
            return

        for df in data:
            if join_on not in df.columns:
                # Key missing, skip or yield based on join type
                yield df if how == "left" else pd.DataFrame()
                continue
                
            try:
                merged = pd.merge(df, lookup_df, on=join_on, how=how)
                yield merged
            except Exception as e:
                 raise ConfigurationError(f"Join operation failed: {e}")
