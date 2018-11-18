import psycopg2 as pg

from StockDataWriter.infrastructure.StockDataQuery import PgStockQuery

class PostgresStockData:

    cursor = None
    conn = None

    def connect_db(self, host, database, user):
        """
        postgres db에 접속한고 cursor를 할당한다
        """
        self.conn = pg.connect(host=host, database=database, user=user)
        self.cursor = self.conn.cursor()

    def save_code(self, code, name):
        query = PgStockQuery.save_item_code(code, name)
        self.cursor.execute(query)
        self.conn.commit()

    def save(self, code, date, open, high, low, close, diff, volume):
        """
        주식 데이터를 저장한다
        """
        # 코드명 및 데이터로 insert 조회쿼리를 만든다
        query = PgStockQuery.save_stock_price(code,
                                              date.date(),
                                              open,
                                              high,
                                              low,
                                              close,
                                              diff,
                                              volume)
        try:
            self.cursor.execute(query)
        except pg.ProgrammingError as e:
            if e.pgcode == '42P01':
                # 만약 noExist exception이 발생하는 경우 해당 테이블을 생성한다
                # 그리고 다시 insert 쿼리를 수행한다
                self.conn.commit()
                self.cursor.execute(PgStockQuery.create_table(code))
                self.cursor.execute(query)

        self.conn.commit()

    def get(self):
        """
        종목 코드별 정보를 가져온다.
        :return:
        """

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.cursor is not None:
            self.cursor.close()
