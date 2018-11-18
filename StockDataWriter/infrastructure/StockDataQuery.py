

class PgStockQuery:
    @staticmethod
    def save_stock_price(code, date, open, high, low, close, diff, volume):
        return 'INSERT into CODE_{0}(date, open, high, low, close, diff, volume)' \
               'values(to_date(\'{1}\', \'YYYY-MM-DD\'), {2}, {3}, {4}, {5}, {6}, {7})'.format(str(code), date, open, high, low, close, diff, volume)

    @staticmethod
    def create_table(code):
        return 'CREATE TABLE CODE_{0}(' \
               'date date NOT NULL,' \
               'open integer NOT NULL,' \
               'high integer NOT NULL,' \
               'low integer NOT NULL,' \
               'close integer NOT NULL,' \
               'diff integer NOT NULL,' \
               'volume bigint NOT NULL)'.format(str(code))

    @staticmethod
    def save_item_code(item, code):
        return 'insert into daily_stock_price_item_code(code, name)' \
               'values(\'{0}\', \'{1}\')'.format(item, code)
