class AppError(Exception):
    """Base exception for the application."""
    def __init__(self, message: str, original_error: Exception = None):
        super().__init__(message)
        self.original_error = original_error

class ConnectorError(AppError):
    """Base exception for connector-related errors."""
    pass

class ConfigurationError(ConnectorError):
    """Raised when connector configuration is invalid."""
    pass

class ConnectionFailedError(ConnectorError):
    """Raised when connection to the external system fails."""
    pass

class AuthenticationError(ConnectionFailedError):
    """Raised when authentication fails."""
    pass

class SchemaDiscoveryError(ConnectorError):
    """Raised when schema discovery fails."""
    pass

class DataTransferError(ConnectorError):
    """Raised during data read/write operations."""
    pass