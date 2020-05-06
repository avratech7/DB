import psycopg2 as pg2
import logging

#tes
class Db:
    # idf - inverse document frequency
    # t = term ביטוי
    # d =  document מסמך
    # D = all documents  קבוצת כל המסמכים
    # N = count(D) כמה מסמכים יש ליd0, d1, d2 ... dn

    connection = None

    def __init__(self, password, host='localhost', user='postgres', database='database'):
        try:
            self.connection = pg2.connect(database="database",
                                          host="host",
                                          user="user",
                                          port="5432",
                                          password=password
                                          )
            print("Opened database successfully")

        except:
            logging.error("connect failed")
            # print(e)

    def connect(self):
        return self.connection

    def disconnect(self):
        self.connection.close()

    def execute(self):
        cur = self.connection.cursor()
        cur.execute("INSERT INTO TF_IDF (ID,TERM ,DOCU,FREQUENCY) VALUES (1, 'SPORT', 'STRING VERY LONG', '5.3' )");
        self.connection.commit()


    def created_table(self):
        cur = self.connection.cursor()
        cur.execute('''CREATE TABLE TF_IDF
              (ID INT PRIMARY KEY     NOT NULL,
              TERM           TEXT    NOT NULL,
              DOCU           TEXT     NOT NULL,
              FREQUENCY      FLOAT,);''')
        print("Table created successfully")

        self.connection.commit()


    def select_from(self):
        try:
            cur = self.connection.cursor()
            cur.execute("""SELECT * FROM TF_IDF""")

            [print(row) for row in cur.fetchall()]
        except:
            logging.error("select failed")


        rows = cur.fetchall()
        for row in rows:
            print(row)
