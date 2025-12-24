from app.connectors.factory import ConnectorFactory
from app.connectors.impl.sql.postgres import PostgresConnector
from app.connectors.impl.sql.sqlite import SQLiteConnector
from app.connectors.impl.sql.mysql import MySQLConnector
from app.connectors.impl.sql.mariadb import MariaDBConnector
from app.connectors.impl.sql.mssql import MSSQLConnector
from app.connectors.impl.sql.oracle import OracleConnector
from app.connectors.impl.sql.redshift import RedshiftConnector
from app.connectors.impl.sql.snowflake import SnowflakeConnector
from app.connectors.impl.sql.bigquery import BigQueryConnector
from app.connectors.impl.files.local import LocalFileConnector
from app.connectors.impl.files.s3 import S3Connector
from app.connectors.impl.nosql.mongodb import MongoDBConnector
from app.connectors.impl.nosql.redis import RedisConnector
from app.connectors.impl.api.rest import RestApiConnector
from app.connectors.impl.generic.custom_script import CustomScriptConnector

# Register all concrete connector implementations
ConnectorFactory.register_connector("postgresql", PostgresConnector)
ConnectorFactory.register_connector("mysql", MySQLConnector)
ConnectorFactory.register_connector("mariadb", MariaDBConnector)
ConnectorFactory.register_connector("mssql", MSSQLConnector)
ConnectorFactory.register_connector("oracle", OracleConnector)
ConnectorFactory.register_connector("redshift", RedshiftConnector)
ConnectorFactory.register_connector("snowflake", SnowflakeConnector)
ConnectorFactory.register_connector("bigquery", BigQueryConnector)
ConnectorFactory.register_connector("sqlite", SQLiteConnector)
ConnectorFactory.register_connector("local_file", LocalFileConnector)
ConnectorFactory.register_connector("s3", S3Connector)
ConnectorFactory.register_connector("mongodb", MongoDBConnector)
ConnectorFactory.register_connector("redis", RedisConnector)
ConnectorFactory.register_connector("rest_api", RestApiConnector)
ConnectorFactory.register_connector("custom_script", CustomScriptConnector)
