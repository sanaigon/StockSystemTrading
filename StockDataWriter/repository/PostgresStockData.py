import psycopg2 as pg


def connect_db():
    conn = pg.connect(host="localhost", database="jkpark", user="jkpark")
    cur = conn.cursor()
    cur.execute("select * from test_stock")
    ret = cur.fetchone()
    return ret


def save():
    """
    주식 데이터를 저장한다
    :return:
    """
    return

def get():
    """
    종목 코드별 정보를 가져온다.
    :return:
    """
