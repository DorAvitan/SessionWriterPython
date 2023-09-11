import sqlalchemy
import os
import logging
import dotenv

dotenv.load_dotenv()
log = logging.getLogger(__name__)

logging.basicConfig(level=logging.INFO)


class SQLClientError(Exception):
    pass


class MSSQLDatabase:
    _connection: sqlalchemy.engine.Connection
    _engine: sqlalchemy.engine

    def __init__(self):
        log.info("Initializing SQL client")
        self._connect_to_databases()

    def __del__(self):
        log.info("Closing database connections")
        try:
            self._connection.close()
            self._engine.dispose()
        except Exception as e:
            log.error(f"Error closing database connections: {str(e)}")

    def _connect_to_databases(self):
        try:
            log.info("Loading config from environment variables")
            server = os.environ.get("MSSQL_SERVER")
            user = os.environ.get("MSSQL_USER")
            password = os.environ.get("MSSQL_PASSWORD")
            db_name = os.environ.get("MSSQL_DB_NAME")

            log.info("Connecting to database")
            self._engine = sqlalchemy.create_engine(f"mssql+pyodbc://{user}:{password}@{server}/{db_name}"
                                                    f"?driver=ODBC+Driver+17+for+SQL+Server")

            self._connection = self._engine.connect()
            log.info("Connected to database")
        except Exception as e:
            raise SQLClientError(f"Database connection error: {str(e)}")

    def _create_table(self, table_name: str, columns: list[str]):
        try:

            self._execute_raw_insert_sql(f"""CREATE TABLE {table_name} (
                                                            {', '.join(columns)}
                                                        )""")
        except Exception as e:
            raise SQLClientError(f"Error creating table: {str(e)}")

    def _drop_table(self, table_name: str):
        try:
            self._execute_raw_insert_sql(f"DROP TABLE IF EXISTS {table_name}")
        except Exception as e:
            raise SQLClientError(f"Error dropping table: {str(e)}")

    def _execute_raw_select_sql(self, sql: str):
        try:
            result = self._connection.execute(sqlalchemy.text(sql))
            rows = result.fetchall()
            return [row._asdict() for row in rows]
        except Exception as e:
            raise SQLClientError(f"Error executing raw SQL query: {str(e)}")

    def _execute_raw_insert_sql(self, sql: str):
        try:
            log.debug(f"Executing raw SQL query: {sql}")
            self._connection.execute(sqlalchemy.text(sql))
            self._connection.commit()
            log.debug("Executed raw SQL query successfully")
        except Exception as e:
            raise SQLClientError(f"Error executing raw SQL query: {str(e)}")
