import uuid

from sql_client import MSSQLDatabase
import logging

log = logging.getLogger(__name__)

class SQLSessionClient(MSSQLDatabase):
    TABLE_NAME = "testSessions"

    def __init__(self, reset_db: bool = True):
        '''
        A dummy db implementation.
        :param reset_db: when True, will drop and recreate the table on every app restart.
        '''
        super().__init__()
        if reset_db:
            log.info("Resetting database")
            self._drop_table(self.TABLE_NAME)
            self._create_table(self.TABLE_NAME, ["timestamp INT NOT NULL",
                                                         "customer_id NVARCHAR(255) NOT NULL",
                                                         "session_id  NVARCHAR(255) PRIMARY KEY NOT NULL",
                                                         "typing_speed FLOAT NOT NULL",
                                                         "cursor_hops INT NOT NULL",
                                                         "ip NVARCHAR(15) NOT NULL",
                                                         "password_pasted BIT NOT NULL",
                                                         "score FLOAT NULL",
                                                         "deleted BIT NOT NULL default 0"])


    def create(self, data: dict) -> dict:
        '''
        Assigns a new random UUID to the session_id field and creates a new session in the database.
        :param data: dict containing all the data to be inserted into the database.
        :return: the session that was created, along with an auto generated session_id (uuid string).
        example usage: my_db.create({"timestamp": 123456789, "customer_id": "Galadriel", "typing_speed": 123.456,
                                    "cursor_hops": 123, "ip": "1.1.1.1", "password_pasted": True})

                                    Will return : {"timestamp": 123456789, "customer_id": "Galadriel",
                                    "typing_speed": 123.456, "cursor_hops": 123, "ip": "1.1.1.1", "password_pasted": True,
                                    "score": null, "session_id": "123e4567-e89b-12d3-a456-426614174000"}
        '''
        data.update({"session_id": str(uuid.uuid4())})
        values = []
        for k, v in data.items():
            if k == "score" and v is None:
                values.append("null")
                continue
            values.append(f"'{v}'")
        sql = f"INSERT INTO {self.TABLE_NAME} ({', '.join(data.keys())}) VALUES ({', '.join(values)})"
        self._execute_raw_insert_sql(sql)
        return data

    def fetch(self, query: dict) -> list[dict]:
        '''
        Fetches all sessions that match the query.
        :param query: dict with the query. example: {"customer_id": "Galadriel", "typing_speed": 123.456}
        :return: list of dicts, each dict is a session representation.
        '''
        sql = f"SELECT * FROM {self.TABLE_NAME} WHERE "
        conditions = []
        for k, v in query.items():
            conditions.append(f"{k} = '{v}'")
        sql += " AND ".join(conditions)
        return self._execute_raw_select_sql(sql)

    def fetch_all(self) -> list[dict]:
        '''
        Fetches all sessions.
        :return: list of dicts, each dict is a session representation.
        '''
        sql = f"SELECT * FROM {self.TABLE_NAME}"
        return self._execute_raw_select_sql(sql)

    def update(self, sid: str, data: dict) -> dict:
        '''
        Updates a session with the given sid with the given data.
        :param sid: session id (uuid str)
        :param data: dict represeting the attributes to update. for example: {"score": 123.456, "deleted": False}
        :return: The updated session data (dict)
        '''
        sql = f"UPDATE {self.TABLE_NAME} SET "
        to_update = []
        for k, v in data.items():
            to_update.append(f"{k} = '{v}'")
        sql += ", ".join(to_update)
        sql += f" WHERE session_id = '{sid}'"
        self._execute_raw_insert_sql(sql)
        res = self.fetch({"session_id": sid})
        return res[0]

    def delete(self, sid: str) -> bool:
        '''
        Deletes from the db
        :param sid:  session id (uuid string)
        :return: bool True if delete succeeded
        '''
        sql = f"DELETE FROM {self.TABLE_NAME} WHERE session_id = '{sid}'"
        self._execute_raw_insert_sql(sql)
        return True
