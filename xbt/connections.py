class DuckDB:
    def __init__(self, conn):
        import duckdb

        self.db = duckdb.connect(conn["path"])

    def execute(self, query):
        self.db.execute(query)


class Postgres:
    def __init__(self, conn):
        import psycopg2

        self.conn = psycopg2.connect(conn["url"])

    def execute(self, query):
        with self.conn.cursor() as cur:
            cur.execute(query)


def build_connection(conn):
    if conn["type"] == "duckdb":
        return DuckDB(conn)

    if conn["type"] == "postgres":
        return Postgres(conn)
