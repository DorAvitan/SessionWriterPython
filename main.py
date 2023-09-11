from sql_dummy_client.sql_sessions_client import SQLSessionClient

# Below is a usage example of the SQLSessionClient class - this is the db that you should use in your task
my_db = SQLSessionClient()  # can add reset_db=False to prevent db from getting deleted on every app restart
my_db.create()
my_db.fetch()
my_db.fetch_all()
my_db.update()
my_db.delete()
