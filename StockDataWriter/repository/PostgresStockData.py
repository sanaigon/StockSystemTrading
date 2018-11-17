import psycopg2 as pg


class PostgresStockData:

    cursor = None

    def connect_db(self, host, database, user):
        """
        postgres db에 접속한고 cursor를 할당한다
        """
        conn = pg.connect(host=host, database=database, user=user)
        self.cursor = conn.cursor()

    def test_function(self):
        self.cursor.execute("select * from daily_stock_price")
        return self.cursor.fetchone()

    def save(self):
        """
        주식 데이터를 저장한다
        :return:
        """
        return

    def get(self):
        """
        종목 코드별 정보를 가져온다.
        :return:
        """

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.cursor is not None:
            self.cursor.close()
