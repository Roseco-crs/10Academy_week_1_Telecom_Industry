
import pandas as pd
import psycopg2
import sqlalchemy
from psycopg2 import sql
import pandas.io.sql as sqlio
from sqlalchemy import engine_create

def db_connection_psycopg():
    # database connection using psycopg2
    pgconn = psycopg2.connect(dbname="telecom", user="postgres", port="5432", host="localhost", password="Postgresql")
    return pgconn

def db_read_table_psycopg(pgconn, table_name):
    sql = f"SELECT * FROM {table_name}"
    df = sqlio.read_sql_query(sql, pgconn)
    return df

def db_write_table_psycopg(pgconn, table_name, df):
    pass

def db_delete_table_psycopg(pgconn, table_name):
    # create a cursor object to execute SQL queries
    cursor = pgconn.cursor()
    # Define the SQL query to drop the table
    drop_table_query = sql.SQL("DROP TABLE IF EXISTS {sql.Idenfier(table_name)} CASCADE")
    # Execute the SQL query
    cursor.execute(drop_table_query)
    pgconn.commit()
    print(f"Table '{table_name}' has been successfully deleted.")
    if cursor:
        cursor.close()

# ---- using sqlalchemy ----------

def db_connection_sqlalchemy():
    engine = create_engine("postgresql+psycopg2://postgres:postgres@localhost:5432/telecom")
    return engine

def db_read_table_sqlalchemy(engine, table_name):
    query = f"SELECT * FROM {table_name}"
    df = pd.read_sql(query, engine)
    return df