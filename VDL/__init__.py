import azure.functions as func
import nest_asyncio
import fastapi
import os
import psycopg2
from typing import Union
from psycopg2.extras import RealDictCursor


app = fastapi.FastAPI()
nest_asyncio.apply()
HOST = os.getenv("DB_HOST")
DBNAME = os.getenv("DB_NAME")
USER = os.getenv("DB_USER")
PASSWORD = os.getenv("DB_PASSWORD")
sslmode = os.getenv("sslmode")

conn_string = "host={0} user={1} dbname={2} password={3} sslmode={4}".format(HOST, USER, DBNAME, PASSWORD, sslmode)
conn = psycopg2.connect(conn_string)
print("Connection established")

cursor = conn.cursor(cursor_factory=RealDictCursor)



@app.get("/api/data-logger")
async def data_logger_index():
    # cursor.execute("SELECT * FROM datalogger;")
    # rows = cursor.fetchall()
    return {
        "data": os.environ,
    }


@app.get("/api/data-logger/{logger_id}")
async def data_logger_view(logger_id: str):
    cursor.execute("SELECT * FROM datalogger WHERE id = '{_logger_id}';".format(_logger_id = logger_id))
    rows = cursor.fetchall()
    return {
        "data": rows,
    }

@app.get("/api/trip-event")
async def data_logger_index(logger_id: Union[str, None] = None, vin: Union[str, None] = None):
    if vin:
        cursor.execute("SELECT * FROM tripevent WHERE VIN = '{_vin}';".format(_vin = vin))

    elif logger_id:
        cursor.execute("SELECT * FROM tripevent WHERE logger_id = '{_logger_id}';".format(_logger_id = logger_id))
    rows = cursor.fetchall()
    return {
        "data": rows,
    }
@app.get("/api/trigger-event")
async def data_logger_index(logger_id: Union[str, None] = None, vin: Union[str, None] = None):
    if vin:
        cursor.execute("SELECT * FROM triggerevent WHERE VIN = '{_vin}';".format(_vin = vin))

    elif logger_id:
        cursor.execute("SELECT * FROM triggerevent WHERE logger_id = '{_logger_id}';".format(_logger_id = logger_id))
    rows = cursor.fetchall()
    return {
        "data": rows,
    }

@app.get("/api/DTC-info")
async def data_logger_index(logger_id: Union[str, None] = None, vin: Union[str, None] = None):
    if vin:
        cursor.execute("SELECT * FROM dtcinfo WHERE VIN = '{_vin}';".format(_vin = vin))

    elif logger_id:
        cursor.execute("SELECT * FROM dtcinfo WHERE logger_id = '{_logger_id}';".format(_logger_id = logger_id))
    rows = cursor.fetchall()
    return {
        "data": rows,
    }
@app.get("/api/file-index")
async def data_logger_index(logger_id: Union[str, None] = None, vin: Union[str, None] = None):
    if vin:
        cursor.execute("SELECT * FROM fileindex WHERE VIN = '{_vin}';".format(_vin = vin))

    elif logger_id:
        cursor.execute("SELECT * FROM fileindex WHERE logger_id = '{_logger_id}';".format(_logger_id = logger_id))
    rows = cursor.fetchall()
    return {
        "data": rows,
    }

@app.get("/api/vehicles")
async def data_logger_index():
    cursor.execute("SELECT * FROM vehicle;")
    rows = cursor.fetchall()
    return {
        "data": rows,
    }
async def main(req: func.HttpRequest, context: func.Context) -> func.HttpResponse:
    """Each request is redirected to the ASGI handler."""
    return func.AsgiMiddleware(app).handle(req, context)