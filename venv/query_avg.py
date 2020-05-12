import sys
import connect as con
cur = con.cursor



def save_tfidf_avg_dict(list_of_dictionaries):
    for dictionaries in list_of_dictionaries:
        term = list(dictionaries.keys())[0]
        cur.execute(f"""SELECT term FROM score WHERE term = '{term}';""")
        if cur.rowcount == 0:
            try:
                cur.execute(f"INSERT INTO score (term,{dictionaries[term][0]},verison_{dictionaries[term][0]}) VALUES ('{term}','{dictionaries[term][1]}',1) ")
                print("term saved successfully")
            except:
               print(sys.exc_info()[1])

        else:
            cur.execute(f"""SELECT verison_{dictionaries[term][0]}, {dictionaries[term][0]} FROM score WHERE term = '{term}';""")
            current_verison = list(cur.fetchall()[0])
            if current_verison[0] is None:
                cur.execute( f"UPDATE score SET ({dictionaries[term][0]}, verison_{dictionaries[term][0]}) = ({dictionaries[term][1]},1) WHERE term = '{term}'")
            else:
                updade_verison = current_verison[0] + 1
                current_avg =  ((current_verison[0] * current_verison[1]) + dictionaries[term][1]) / updade_verison
                cur.execute( f"UPDATE score SET ({dictionaries[term][0]}, verison_{dictionaries[term][0]}) = ({current_avg},{updade_verison}) WHERE term = '{term}'")


def save_tfidf_avg(list_of_listes):
    for _list in list_of_listes:
        term = _list[0]
        cur.execute(f"""SELECT term FROM score WHERE term = '{term}';""")
        if cur.rowcount == 0:
            try:
                cur.execute(f"INSERT INTO score (term,{_list[1]},verison_{_list[1]}) VALUES ('{term}','{_list[2]}',1) ")
                print("term saved successfully")
            except:
               print(sys.exc_info()[1])

        else:
            cur.execute(f"""SELECT verison_{_list[1]}, {_list[1]} FROM score WHERE term = '{term}';""")
            current_verison = list(cur.fetchall()[0])
            if current_verison[0] is None:
                cur.execute(f"UPDATE score SET ({_list[1]}, verison_{_list[1]}) = ({_list[2]},1) WHERE term = '{term}'")
            else:
                updade_verison = current_verison[0] + 1
                current_avg =  ((current_verison[0] * current_verison[1]) + _list[2]) / updade_verison
                cur.execute( f"UPDATE score SET ({_list[1]}, verison_{_list[1]}) = ({current_avg},{updade_verison}) WHERE term = '{term}'")



def get_tfidf_avg(**dictionery):

    if not dictionery:
      return  con.return_tables("""SELECT * FROM score""")
    else:
        keys = list(dictionery.keys())
        term = ""
        label = ""
        for k in keys:
            if k == "term":
                term = k
            if k == "label":
                label = k
        if len(dictionery) == 1:
            if term == "term":
                try:
                    cur.execute(f"""SELECT * FROM score WHERE term = '{dictionery[term]}';""")
                    if cur.rowcount > 0:
                      return  con.return_tables(f"""SELECT * FROM score WHERE term = '{dictionery[term]}';""")
                    else:
                        print("The term does not exist")
                except:
                    print(sys.exc_info()[1])

            elif label == "label":
                try:
                   return con.return_tables(f"""SELECT term, {dictionery[label]} FROM score;""""")
                except:
                    print(sys.exc_info()[1])
            else:
                print(f" is not key Only label or term must be defined")

        else:
            try:
                cur.execute(f"""SELECT {dictionery[label]} FROM score WHERE term = '{dictionery[term]}';""")
                if cur.rowcount > 0:
                    rows = cur.fetchall()
                    result = []
                    [result.append(row[0]) for row in rows]
                    return result[0]
                else:
                    print("The term or label does not exist")
            except:
                print(f" is not key Only label or term must be defined")


if __name__ == '___main__':
    con.conn.commit()
    cur.close()
    con.conn.close()