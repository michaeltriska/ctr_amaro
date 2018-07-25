import sqlite3 as sqlite
import pandas as pd

class DataBaseCreator:

    def __init__(self, database_name):
        self.connection = sqlite.connect(database_name)
        self.cur = self.connection.cursor()

    def create_database(self, table_names):

        for table in table_names:
            self.create_table(table)
        print('database created')
        self.connection.close()

    def create_table(self, tablename):
        df_orders = self.read_csv('data/' + tablename + '.csv')
        if 'order_date' in df_orders.columns.values:
            df_orders['order_date'] = pd.to_datetime(df_orders['order_date'],
                                                     format='%d/%m/%Y %H:%M')
        try:
            df_orders.to_sql(tablename, self.connection)

        except:
            pass

    def read_csv(self, file_name):
        return pd.read_csv(file_name, sep=';',
                           index_col=None,
                           infer_datetime_format=True)

if __name__ == '__main__':
    db = DataBaseCreator('connectors/orders.db')
    db.create_database(['orders', 'order_items'])
