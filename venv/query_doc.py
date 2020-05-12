import sys
import connect as con
cur = con.cursor



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


if __name__ == '___main__':
    con.conn.commit()
    cur.close()
    con.conn.close()