from abc import ABC, abstractmethod
from typing import Any, Dict, Iterator, List, Optional, Union, Generator
from contextlib import contextmanager
import pandas as pd


class BaseConnector(ABC):
    """
    Abstract Base Class for all data connectors (Sources and Destinations).
    Enforces a standard interface for connection management, schema discovery,
    and data transfer (IO).
    """

    def __init__(self, config: Dict[str, Any]):
        """
        Initialize the connector with configuration settings.
        
        Args:
            config: Dictionary containing connection parameters (host, port, credentials, etc.)
        """
        self.config = config
        self.validate_config()

    @abstractmethod
    def validate_config(self, **kwargs) -> None:
        """
        Validate the configuration dictionary.
        Should raise ConfigurationError if invalid.
        """
        pass

    @abstractmethod
    def connect(self, **kwargs) -> None:
        """
        Establish the connection to the external system.
        Should raise ConnectionFailedError or AuthenticationError on failure.
        """
        pass

    @abstractmethod
    def disconnect(self, **kwargs) -> None:
        """
        Close the connection and release resources.
        """
        pass

    @abstractmethod
    def test_connection(self, **kwargs) -> bool:
        """
        Test if the connection can be established successfully.
        Returns True if successful, raises an exception or returns False otherwise.
        """
        pass

    @contextmanager
    def session(self) -> Generator["BaseConnector", None, None]:
        """
        Context manager to ensure safe connection handling.
        
        Usage:
            with connector.session() as conn:
                conn.read(...)
        """
        try:
            self.connect()
            yield self
        finally:
            self.disconnect()


    @abstractmethod
    def list_assets(self, **kwargs) -> List[str]:
        """
        List available assets (tables, buckets, files) in the source.
        """
        pass

    @abstractmethod
    def get_schema(self, asset: str) -> Dict[str, Any]:
        """
        Get the schema definition for a specific asset.
        """
        pass

    # -------------------------------------------------------------------------
    # Data IO
    # -------------------------------------------------------------------------

    @abstractmethod
    def read_batch(
        self, 
        asset: str, 
        limit: Optional[int] = None, 
        offset: Optional[int] = None,
        **kwargs
    ) -> Iterator[pd.DataFrame]:
        """
        Read data from the source in batches.
        
        Args:
            asset: The table name, file path, or resource identifier.
            limit: Max number of records to read (optional).
            offset: Number of records to skip (optional).
            **kwargs: Additional read options (filters, columns, etc.).
            
        Yields:
            pandas.DataFrame: Chunks of data.
        """
        pass

    @abstractmethod
    def write_batch(
        self, 
        data: Union[pd.DataFrame, Iterator[pd.DataFrame]], 
        asset: str, 
        mode: str = "append",
        **kwargs
    ) -> int:
        """
        Write data to the destination.

        Args:
            data: A single DataFrame or an iterator of DataFrames.
            asset: The target table name or file path.
            mode: Write mode ('append', 'replace', 'upsert').
            **kwargs: Additional write options.

        Returns:
            int: Total number of records written.
        """
        pass