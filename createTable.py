from util import database as db

conn = db.open_conn()

db._createTableDiario(conn)