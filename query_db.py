import psycopg2
import logging
import connect
from collections import namedtuple
cur = connect.cursor;
def create_record(obj, fields):
    ''' given obj from db returns named tuple with fields mapped to values '''
    Score = namedtuple("Score", fields)
    mappings = dict(zip(fields, obj))
    return Score(**mappings)
def save_docs(list):
        key = list[0]
        cur.execute(f"INSERT INTO all_docs (docs, label) VALUES (%s, %s);", (list[0], list[1]))
def save_tfidf(list_of_dictionaries):
    for dictionaries in list_of_dictionaries:
        key = list(dictionaries.keys())[0]
        cur.execute(f"""SELECT term FROM score WHERE term = '{key}';""")
        if cur.rowcount == 0:
            connect.insert("score", key, dictionaries[key][1],dictionaries[key][0])
        else:
            connect.update("score", key,dictionaries[key][0], dictionaries[key][1])
#
def get_tfidf(**label):
    if not label:
        cur.execute("""SELECT * FROM score""")
        colnames = [desc[0] for desc in cur.description]
        rows = cur.fetchall()
        result = []
        [result.append(create_record(row, colnames)) for row in rows]
        print(result)
    else:
        t = list(label.keys())
        for i in t:
            if i == "term":
                try:
                    cur.execute(f"""SELECT * FROM score WHERE term = '{label[i]}';""")
                except:
                    logging.error("select failed")
                if cur.rowcount > 0 :
                    if len(label)==1:
                        colnames = [desc[0] for desc in cur.description]
                        rows = cur.fetchall()
                        result = []
                        [result.append(create_record(row, colnames)) for row in rows]
                        print(result)
                else:
                    print("The term does not exist in the table")
            elif i == "label":
                try:
                    cur.execute(f"""SELECT term, {label[i]} FROM score;""""")
                    colnames = [desc[0] for desc in cur.description]
                    rows = cur.fetchall()
                    result = []
                    [result.append(create_record(row, colnames)) for row in rows]
                    print(result)
                except psycopg2.errors.UndefinedColumn as e:
                    print(e)
            else:
                print(f"{i} is not key Only label or term must be defined")
def get_count_doc(label):
    cur.execute(f"""SELECT * FROM all_docs WHERE label = '{label}';""")
    return cur.rowcount

connect.conn.commit()
connect.cursor.close()
connect.conn.close()