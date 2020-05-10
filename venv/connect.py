import psycopg2
import logging

# Update connection string information
host = "drona.db.elephantsql.com"
dbname = "lnhiqqex"
user = "lnhiqqex"
password = "iP6W0C7_-6rsUI9dK7JN7WI6qxPVEx-q"

# Construct connection string
def connet_to_host(host,user,dbname,password):
    conn_string = f"host={host} user={user} dbname={dbname} password={password}"
    try:
        conn  = psycopg2.connect(conn_string)
        print("Connection established")
        return conn
    except:
        logging.error("connection failed")
conn = connet_to_host(host,user,dbname,password)
cursor = conn.cursor()

# Create a table
def create_a_table(table_name,list_of_cloum,definition):
    try:
        cursor.execute(f"CREATE TABLE  {table_name}({list_of_cloum} {definition});")
        print("Finished creating table")
    except psycopg2.errors.DuplicateTable as p:
        print(p)

def add_cloum(table_name, cloum_name, definition):
    cursor.execute(f"ALTER TABLE {table_name} ADD COLUMN {cloum_name} {definition};")

def drop_table(cur,table_name):
    cur.execute(f"DROP TABLE IF EXISTS {table_name};")
    print("Finished dropping table (if existed)")

def insert(table_name,cloum_name,value):
    cursor.execute(f"INSERT INTO {table_name} ({cloum_name}) VALUES {value};")

def update_were(table_name,cloum_name,value,cloum_to_comp,value_to_comp):
    cursor.execute(f"UPDATE {table_name} SET {cloum_name} = {value} WHERE {cloum_to_comp} = '{value_to_comp}'")

def print_tables(cur, select_string):
    cur.execute(select_string)
    colnames = [desc[0] for desc in cur.description]
    rows = cur.fetchall()
    result = []
    [result.append(dict(zip(colnames, row))) for row in rows]
    for item in result:
        print(item)

def select_all(TABLE='NEW_TABLE'):
    try:
        print_tables(cursor,f"select * from {TABLE}")
    except psycopg2.errors.UndefinedTable as p:
        print(p)

conn.commit()

