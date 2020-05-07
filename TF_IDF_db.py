import psycopg2

conn = psycopg2.connect("dbname='postgres' user='postgres'  host='localhost'")
cur = conn.cursor()


def created_table():
    cur.execute("CREATE TABLE tf_idf ID INT PRIMARY KEY     NOT NULL TERM           TEXT    NOT NULL, DOCU           TEXT     NOT NULL, FREQUENCY      FLOAT,);")
    print("Table created successfully")

def insert_a(table = 'NEW_TABLE',culom='A' ,value = 0):
    cur.execute(f"insert into {table} ({culom}) values ({value})" )

def join_all():
    cur.execute("select * from words_pass as w join chaim_projct as c on w.word = c.word")


def select_all( NEW_TABLE =  'NEW_TABLE' ):
    cur.execute(f"select * from {NEW_TABLE}")

insert_a(value = 3)
conn.commit()
[print(row) for row in cur.fetchall()]

conn.close()


