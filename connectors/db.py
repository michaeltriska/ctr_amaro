import sqlite3 as sqlite
from datetime import datetime
import pandas as pd
from datetime import timedelta

class DataBaseHandler:
    def __init__(self, database_name):
        self.database = database_name
        self.connection = self._get_connection()
        self.cur = self.connection.cursor()

    def _get_connection(self):
        return sqlite.connect(self.database)

    def close_connection(self):
        self.connection.close()

    def get_purchase_df(self, start, end, product, platform):

        sql_query = self.get_purchase_query(
            start, end, product, platform)
        self.cur.execute(sql_query)
        rows = self.cur.fetchall()
        cols = [description[0]
                for description
                in self.cur.description]
        self.connection.close()

        df_oders = pd.DataFrame(rows, columns=cols)
        df_oders["order_date"] = [datetime.strptime(
                                  date, '%Y-%m-%d %H:%M:%S') +
                                  timedelta(days=700)
                                  for date in df_oders.order_date]
        return df_oders

    def get_purchase_query(self, start, end, product, platform):
        sql_query = """
                    SELECT
                               orders.order_date AS "order_date",
                               orders.device_type AS "device_type",
                               order_items.code_color AS "code_color",
                               orders.id AS "order_id"
                    FROM
                               orders
                    INNER JOIN
                               order_items
                    ON
                               orders.id = order_items.order_id
                    WHERE
                               order_date >= strftime('%s')
                               AND order_date < strftime('%s')
                    """ % (start, end)

        if product:
            sql_query += ' AND order_items.code_color == "' + \
                         product + '"'

        if platform:
            sql_query += ' AND orders.device_type == "' + \
                         platform + '"'
        return sql_query
