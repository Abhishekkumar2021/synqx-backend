from abc import ABC, abstractmethod
from typing import Any, Dict, Iterator
import pandas as pd

class BaseTransform(ABC):
    """
    Abstract Base Class for data transformations.
    """

    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.validate_config()

    @abstractmethod
    def validate_config(self) -> None:
        """
        Validate the configuration.
        """
        pass

    @abstractmethod
    def transform(self, data: Iterator[pd.DataFrame]) -> Iterator[pd.DataFrame]:
        """
        Apply transformation to the data stream.
        """
        pass
