from app.connectors.factory import ConnectorFactory
from app.connectors.impl.sql.postgres import PostgresConnector
from app.connectors.impl.sql.sqlite import SQLiteConnector
from app.connectors.impl.files.local import LocalFileConnector

# Register all concrete connector implementations
ConnectorFactory.register_connector("postgresql", PostgresConnector)
ConnectorFactory.register_connector("sqlite", SQLiteConnector)
ConnectorFactory.register_connector("local_file", LocalFileConnector)
