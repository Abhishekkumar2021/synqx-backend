from app.connectors.factory import ConnectorFactory
from app.connectors.impl.sql.postgres import PostgresConnector

# Register all concrete connector implementations
ConnectorFactory.register_connector("postgresql", PostgresConnector)

# Other connectors will be registered here as they are implemented
# from app.connectors.impl.files.s3 import S3Connector
# ConnectorFactory.register_connector("s3", S3Connector)
