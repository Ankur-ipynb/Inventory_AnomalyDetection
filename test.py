import unittest
import sqlite3
import pandas as pd
from unittest.mock import patch
from notebooks.ingest import generate_inventory_data, ingest_to_sqlite

class TestIngest(unittest.TestCase):
    def setUp(self):
        # Set up an in-memory SQLite database for testing
        self.conn = sqlite3.connect(":memory:")
        self.cursor = self.conn.cursor()
        self.cursor.execute('''
            CREATE TABLE inventory (
                product_id TEXT,
                quantity INTEGER,
                price REAL,
                event_timestamp TEXT,
                store_location TEXT,
                category TEXT
            )
        ''')
        self.conn.commit()

    def tearDown(self):
        # Clean up the in-memory database
        self.conn.close()

    def test_generate_inventory_data(self):
        # Test data generation
        df = generate_inventory_data(num_records=5)
        self.assertEqual(len(df), 5)
        self.assertTrue(all(col in df.columns for col in [
            'product_id', 'quantity', 'price', 'event_timestamp',
            'store_location', 'category'
        ]))
        self.assertTrue(all(df['quantity'].between(1, 100)))
        self.assertTrue(all(df['price'].between(10, 500)))

    @patch('notebooks.ingest.sqlite3.connect')
    def test_ingest_to_sqlite(self, mock_connect):
        # Mock the SQLite connection to use the in-memory database
        mock_connect.return_value = self.conn

        # Generate test data
        df = pd.DataFrame({
            'product_id': ['P001', 'P002'],
            'quantity': [10, 20],
            'price': [100.0, 200.0],
            'event_timestamp': ['2025-05-28T15:00:00Z', '2025-05-28T15:01:00Z'],
            'store_location': ['NYC', 'LA'],
            'category': ['Electronics', 'Clothing']
        })

        # Ingest data
        ingest_to_sqlite(df)

        # Verify data in the in-memory database
        result = pd.read_sql_query("SELECT * FROM inventory", self.conn)
        self.assertEqual(len(result), 2)
        self.assertEqual(result.iloc[0]['product_id'], 'P001')
        self.assertEqual(result.iloc[1]['store_location'], 'LA')

if __name__ == '__main__':
    unittest.main()
