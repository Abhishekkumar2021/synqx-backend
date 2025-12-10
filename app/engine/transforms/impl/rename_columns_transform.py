from typing import Iterator, Dict, Any, List
import pandas as pd
from app.engine.transforms.base import BaseTransform
from app.core.errors import ConfigurationError

class RenameColumnsTransform(BaseTransform):
    """
    Renames columns based on a provided mapping.
    Config:
    - rename_map: Dict[str, str] (e.g., {"old_name": "new_name"})
    """

    def validate_config(self) -> None:
        if "rename_map" not in self.config or not isinstance(self.config["rename_map"], dict):
            raise ConfigurationError("RenameColumnsTransform requires 'rename_map' as a dictionary in config.")

    def transform(self, data: Iterator[pd.DataFrame]) -> Iterator[pd.DataFrame]:
        rename_map = self.config["rename_map"]
        for df in data:
            yield df.rename(columns=rename_map)
