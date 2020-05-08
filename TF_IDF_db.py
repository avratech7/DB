import psycopg2
import logging

# Update connection string information
host = "drona.db.elephantsql.com"
dbname = "lnhiqqex"
user = "lnhiqqex"
password = "iP6W0C7_-6rsUI9dK7JN7WI6qxPVEx-q"

# Construct connection string
conn_string = f"host={host} user={user} dbname={dbname} password={password} "
try:
    conn = psycopg2.connect(conn_string)
    print("Connection established")
except:
    logging.error("connection failed")

cursor = conn.cursor()


# Create  new a table
def create_new_table(new_table, culome):
    print(culome)
    try:
        cursor.execute(f"CREATE TABLE {new_table}"
                       "(id serial PRIMARY KEY,"
                       f"{culome});")
        print(f"Finished creating table {new_table} and culoms {culome}")
    except:
        logging.error(f"creating table {new_table} failed and culoms {culome}")


def select_all(TABLE):
    cur.execute(f"select * from {TABLE}")


def insert_into(table='idf', culomA='term', culomB='score', valueA='naon', valueB='0'):
    try:
        cursor.execute(f"insert into {table} ({culomA}, {culomB}) values ('{valueA}' ,{valueB})")
        print(f'insert into {table} ({culomA}, {culomB}) values ({valueA} ,{valueB})')
        select_table(table)
    except:
        logging.error("insert  failed")
        

def join_table(tableA, tableB, word):
    cur.execute(f"select * from {tableA} as a "
                f"join {tableB} as b "
                f"on a.{word} = b.{word}")


conn.commit()
[print(row) for row in cur.fetchall()]

conn.close()
