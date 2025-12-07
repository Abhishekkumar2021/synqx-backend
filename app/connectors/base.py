from abc import ABC, abstractmethod
from typing import Any, Dict, Optional, List, Iterator, Union, Generator
from contextlib import contextmanager
import pandas as pd


class BaseConnector(ABC):
    """
    Abstract Base Class for all data connectors (Sources and Destinations).
    Enforces a standard interface for connection management, schema discovery,
    and data transfer (IO).
    """

    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.validate_config()

    @abstractmethod
    def validate_config(self) -> None:
        pass

    @abstractmethod
    def connect(self) -> None:
        pass

    @abstractmethod
    def disconnect(self) -> None:
        pass

    @abstractmethod
    def test_connection(self) -> bool:
        pass

    @contextmanager
    def session(self) -> Generator["BaseConnector", None, None]:
        self.connect()
        try:
            yield self
        finally:
            self.disconnect()

    @abstractmethod
    def discover_assets(
        self, pattern: Optional[str] = None, include_metadata: bool = False, **kwargs
    ) -> List[Dict[str, Any]]:
        pass

    @abstractmethod
    def infer_schema(
        self,
        asset: str,
        sample_size: int = 1000,
        mode: str = "auto",  # "metadata", "sample", or "auto"
        **kwargs,
    ) -> Dict[str, Any]:
        pass
    
    @abstractmethod
    def read_batch(
        self,
        asset: str,
        limit: Optional[int] = None,
        offset: Optional[int] = None,
        **kwargs,
    ) -> Iterator[pd.DataFrame]:
        pass

    @abstractmethod
    def write_batch(
        self,
        data: Union[pd.DataFrame, Iterator[pd.DataFrame]],
        asset: str,
        mode: str = "append",
        **kwargs,
    ) -> int:
        pass

    @staticmethod
    def slice_dataframe(df: pd.DataFrame, offset: Optional[int], limit: Optional[int]):
        if offset:
            df = df.iloc[offset:]
        if limit:
            df = df.iloc[:limit]
        return df

    @staticmethod
    def chunk_dataframe(df: pd.DataFrame, chunksize: int):
        for i in range(0, len(df), chunksize):
            yield df.iloc[i : i + chunksize]
