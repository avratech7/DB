import psycopg2
import logging
import sys
import connect as con
cur = con.cursor

def save_docs(doc,label):
        try:
            cur.execute(f"INSERT INTO all_docs (doc,label) VALUES ('{doc}','{label}') ")
            print("Document saved successfully")
        except:
           print(sys.exc_info()[1])



def get_docs():
        con.print_tables("""SELECT * FROM all_docs""")
        cur.execute("""SELECT doc, label FROM all_docs""")
        rows = cur.fetchall()
        result = []
        [result.append(list(row)) for row in rows]
        [print(item) for item in result]

def get_doc_by_label(label):
         con.print_tables(f"""SELECT doc, label FROM all_docs WHERE label= '{label}';""")

def save_tfidf(list_tfidf):
    for _list in list_tfidf:
        try:
             cur.execute(f"INSERT INTO tfidf (term,score,label) VALUES ('{_list[0]}',{_list[1]},'{_list[2]}') ")
             print("Score saved successfully")
        except:
             print(sys.exc_info()[1])


def get_all_tfidf():
    con.print_tables("""SELECT * FROM tfidf""")



def get_score_term_by_label(list_tfidf):
    cur.execute(f"""SELECT score FROM tfidf WHERE term= '{list_tfidf[0]}' AND label = '{list_tfidf[1]}';""")
    rows = cur.fetchall()
    result = []
    [result.append(row[0]) for row in rows]
    print(result)




def get_count_doc(label):
    cur.execute(f"""SELECT * FROM all_docs WHERE label = '{label}';""")
    return cur.rowcount




# save_docs("this is corona", "medicine")
# save_docs("this is tenis", "sport")
# save_docs("this is fsddsd", "other")
# save_docs("this is ball", "sport")
# save_docs("this is ball", 5)
list_tfidf = [["ball",0.5,"sport"],["corona" ,0.3, "medicine"],["corona" ,0.2, "medicine"]]



# save_tfidf(list_tfidf)

get_all_tfidf()
get_score_term_by_label(["corona","medicine"])
get_docs()
get_doc_by_label("sport")

con.conn.commit()
cur.close()
con.conn.close()