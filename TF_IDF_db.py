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

doc1 = [{'lebel': 'sport', 'term': 'wow', 'score': 0.59},
        {'lebel': 'sport', 'term': 'this', 'score': 0.04},
        {'lebel': 'sport', 'term': 'what', 'score': 0.61},
        {'lebel': 'sport', 'term': 'paly', 'score': 0.91},
        {'lebel': 'sport', 'term': 'amazing', 'score': 0.65},
        {'lebel': 'sport', 'term': 'really', 'score': 0.04},
        {'lebel': 'sport', 'term': 'cool', 'score': 0.61},
        {'lebel': 'sport', 'term': 'gams', 'score': 0.91},
        {'lebel': 'sport', 'term': 'magic', 'score': 0.65},
        {'lebel': 'sport', 'term': 'wonderful', 'score': 0.71}]

doc2 = [{'lebel': 'sport', 'term': 'a', 'score': 0.99},
        {'lebel': 'sport', 'term': 'this', 'score': 5.77},
        {'lebel': 'sport', 'term': 'whata', 'score': 7.61},
        {'lebel': 'sport', 'term': 'pagfly', 'score': 50.91},
        {'lebel': 'sport', 'term': 'pygtfhon', 'score': 5.75},
        {'lebel': 'sport', 'term': 'javhfa', 'score': 8.71}]

cursor = conn.cursor()


# Create  new a table
def create_new_table(new_table, colum):
    print(colum)
    try:
        cursor.execute(f"CREATE TABLE {new_table}"
                       "(id serial PRIMARY KEY,"
                       f"{colum});")
        print(f"Finished creating table {new_table} and culoms {colum}")
    except:
        logging.error(f"creating table {new_table} failed and culoms {colum}")


'''create_new_table('tfidf_test',"label VARCHAR , term VARCHAR, score FLOAT")'''


# add  where
def get_table(TABLE, colum='*'):
    cursor.execute(f"select {colum} from {TABLE} ")


def insert_into(table, culomns="term,score", values="non,0"):
    try:
        cursor.execute(f"insert into {table} ({culomns}) values ({values})")
        print(f'insert into {table} ({culomns}) values ({values})')
        get_table(table)
    except:
        logging.error(f"insert  failed {table} ({culomns}) values ('{values}')")


def updata(term, score, table="tfidf_test", culom_set='score', where_itmes="term"):
    cursor.execute(f"UPDATE {table} "
                   f"SET {culom_set} = %s "
                   f"WHERE {where_itmes} = %s", (score, term))


def get_row_by_item(colum, item, table = "tfidf_test"):
    cursor.execute(f"select *  from {table} " f"WHERE {colum} ='{item}'")
    return cursor.fetchone()


def save_tfidf(doc):
    print(doc)
    for k in doc:
        l = k['lebel']
        t = k['term']
        s = k['score']

        sql = get_row_by_item('term', t)
        # print

        if sql == None:
            print(sql, t)
            insert_into('tfidf_test', 'label, term , score', f"'{l}','{t}',{s}")
        else:
            old_score = (sql[3])
            # print(type(old_s))
            print("old score", sql, "new  score ", (old_score + s) / 2)
            updata(t, ((old_score) + s) / 2)

    #cursor.execute("SELECT * FROM tfidf_test ORDER BY score")


save_tfidf(doc1)


def join_table(tableA, tableB, word):
    cursor.execute(f"select * from {tableA} as a "
                   f"join {tableB} as b "
                   f"on a.{word} = b.{word}")


def DELETE_SQL():
    sql = "DROP TABLE tfidf_test"
    sql = "DELETE FROM tfidf_test WHERE score < 1"
    cursor.execute(sql)


# DELETE_SQL()
conn.commit()

# select_table("tfidf_test","term")
get_table('tfidf_test')
# get_table('score')
[print(row) for row in cursor.fetchall()]

cursor.close()

conn.close()
