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
    conn  = psycopg2.connect(conn_string)
    print("Connection established")
except:
    logging.error("connection failed")


cursor = conn.cursor()

# Create a table
try:
    cursor.execute("CREATE TABLE all_docs(doc_id serial PRIMARY KEY, docs TEXT, label VARCHAR);")
    print("Finished creating table all_doc")
except:
    logging.error("creating table all_doc failed")

try:
    cursor.execute("CREATE TABLE tf(term VARCHAR, score FLOAT);")
    print("Finished creating table tf")
except:
    logging.error("creating table tf failed")

try:
    cursor.execute("CREATE TABLE idf(doc_id INT, term VARCHAR, score FLOAT);")
    print("Finished creating table idf")
except:
    logging.error("creating table idf failed")

try:
    cursor.execute("CREATE TABLE score(term VARCHAR, sport FLOAT, machine FLOAT, other FLOAT);")
    print("Finished creating table score")
except:
    logging.error("creating table score failed")
conn.commit()
cursor.close()
conn.close()