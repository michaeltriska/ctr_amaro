import unittest
from connectors.db import DataBaseHandler

class DataBaseTests(unittest.TestCase):

    def test_database(self):
        db = DataBaseHandler('connectors/orders.db')
        rows = db.cur.execute("SELECT * FROM orders")
        assert rows.fetchall()[0] == (0, 144296, 16423318,
                                     '2016-02-01 00:11:00', 234.8, 0.0,
                                     234.8, 'DELIVERED', 'CREDIT CARD',
                                     15.39, 'Correios PAC', 'Brasília',
                                     'DF', 'google / organic', 'iOS')

if __name__ == '__main__':
    unittest.main()
