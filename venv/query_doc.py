import sys
import connect as con
cur = con.cursor


def save_docs_into(doc, label):
    string_label = ""

    for i in label:
        string_label = i

    for i in doc:
        if not (i == ""):
def get_docs():
        cur.execute("""SELECT doc, label FROM all_docs""")
        rows = cur.fetchall()
        result = []
        [result.append(list(row)) for row in rows]
        return result


def save_tfidf(list_tfidf):
    for _list in list_tfidf:
        try:
             cur.execute(f"INSERT INTO tfidf (term,score,label) VALUES ('{_list[0]}',{_list[1]},'{_list[2]}') ")
             print("Score saved successfully")
        except:
             print(sys.exc_info()[1])

con.conn.commit()
if __name__ == '___main__':
    cur.close()
    con.conn.close()
