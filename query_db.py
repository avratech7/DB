import psycopg2
import logging
import connect
from collections import namedtuple
cur = connect.cursor;

def save_docs(doc,label):
        connect.insert("all_docs","doc",doc)
        connect.insert("all_docs", "label", doc)


def get_docs():
        connect.print_tables(cur,"""SELECT * FROM all_docs""")

def get_doc_by_label(label):
         connect.print_tables(cur,f"""SELECT docs, label FROM all_docs WHERE label= '{label}';""")

def save_tfidf(list_of_dictionaries):
    for dictionaries in list_of_dictionaries:
        term = list(dictionaries.keys())[0]
        cur.execute(f"""SELECT term FROM score WHERE term = '{term}';""")
        if cur.rowcount == 0:
            cur.execute(f"INSERT INTO score (term,{dictionaries[term][0]}) VALUES ({term},{dictionaries[term][1]}) ")
        else:
            cur.execute(f"""SELECT verison, {dictionaries[term][0]} FROM score WHERE term = '{term}';""")
            current_verison = list(cur.fetchall()[0])
            updade_verison = current_verison[0] + 1
            current_avg =  ((current_verison[0] * current_verison[1]) + dictionaries[term][1]) / updade_verison
            cur.execute( f"UPDATE score SET ({dictionaries[term][0]}, verison) = ({dictionaries[term][1]},{current_avg}) WHERE term = '{term}'")
#
def get_tfidf(**label):
    if not label:
        connect.print_tables(cur,"""SELECT * FROM score""")
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
                        connect.print_tables(cur,f"""SELECT * FROM score WHERE term = '{label[i]}';""")
                else:
                    print("The term does not exist in the table")
            elif i == "label":
                try:
                    connect.print_tables(cur,f"""SELECT term, {label[i]} FROM score;""""")
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
