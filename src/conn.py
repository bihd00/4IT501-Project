import os
import pyodbc


_CONNECTION_STRING = (
    'Driver=ODBC Driver 17 for SQL Server;'
    'Server={DB_SERVER};'
    'Database={DB_NAME};'
    'Trusted_Connection=yes;'
    'Encrypt=no;'
    'TrustServerCertificate=no;'
    'Connection Timeout=30;'
    'unicode_results=True'
)

CONNECTION_STRING = _CONNECTION_STRING.format(
    DB_SERVER=os.environ.get("DB_SERVER", ""),
    DB_NAME=os.environ.get("DB_NAME", "")
)


def get_connection(db_server: str, db_name: str) -> pyodbc.Connection:
    cxs = _CONNECTION_STRING.format(
        DB_SERVER=db_server,
        DB_NAME=db_name
    )
    return pyodbc.connect(cxs)


if __name__ == "__main__":
    print('[CONNECTION_STRING]', CONNECTION_STRING)
    conn = pyodbc.connect(CONNECTION_STRING)
    curr = conn.cursor()
    curr.execute('SELECT 1 AS test')
    resp = curr.fetchone()
    assert resp is not None, f'{resp}'
    print('[CONNECTED]')