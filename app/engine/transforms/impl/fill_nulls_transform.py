from typing import Iterator, Dict, Any, List, Literal
import pandas as pd
from app.engine.transforms.base import BaseTransform
from app.core.errors import ConfigurationError

class FillNullsTransform(BaseTransform):
    """
    Fills null (NaN) values in the DataFrame.
    Config:
    - value: Any (Value to fill nulls with, e.g., 0, 'N/A', -1) OR
    - strategy: Literal['mean', 'median', 'mode', 'ffill', 'bfill'] (Method to fill nulls)
    - subset: Optional[List[str]] (Columns to apply fill to, default all)
    """

    def validate_config(self) -> None:
        if "value" not in self.config and "strategy" not in self.config:
            raise ConfigurationError("FillNullsTransform requires either 'value' or 'strategy' in config.")
        if "value" in self.config and "strategy" in self.config:
            raise ConfigurationError("FillNullsTransform cannot have both 'value' and 'strategy' specified.")
        if "subset" in self.config and not isinstance(self.config["subset"], list):
            raise ConfigurationError("FillNullsTransform 'subset' must be a list of column names.")

    def transform(self, data: Iterator[pd.DataFrame]) -> Iterator[pd.DataFrame]:
        value = self.config.get("value")
        strategy = self.config.get("strategy")
        subset_cols = self.config.get("subset")

        for df in data:
            target_cols = [col for col in subset_cols if col in df.columns] if subset_cols else df.columns

            if value is not None:
                df[target_cols] = df[target_cols].fillna(value=value)
            elif strategy:
                if strategy == 'mean':
                    df[target_cols] = df[target_cols].fillna(df[target_cols].mean())
                elif strategy == 'median':
                    df[target_cols] = df[target_cols].fillna(df[target_cols].median())
                elif strategy == 'mode': # Mode can return multiple values, take the first
                    df[target_cols] = df[target_cols].fillna(df[target_cols].mode().iloc[0])
                elif strategy == 'ffill':
                    df[target_cols] = df[target_cols].ffill()
                elif strategy == 'bfill':
                    df[target_cols] = df[target_cols].bfill()
                else:
                    raise ConfigurationError(f"Unsupported fill strategy: {strategy}")
            yield df
