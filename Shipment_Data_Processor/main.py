import sqlite3
import pandas as pd

"""
The aim of this project is to access data from 3 seperate unorganised CSV files and insert them into a new SQLite3 database.

'Shipping_data_0.csv' is an independant file, the other 2 are dependant on eachother.

DATA:
1. Product Name is provided in both data frames
2. DriverID is unrequired
3. Origin is provided in both data frames
4. Destination is provided in both data frames
5. The product quantity is available in the first dataframe. 
6. The product quanity will be needed to be counted in shipment_data_1.csv.

"""

class Solution:
    def __init__(self, database_file):
        self.connection = sqlite3.connect(database_file)
        self.cursor = self.connection.cursor()

    def read_and_populate(self):
        # Read the CSV files
        shipping_data_0 = pd.read_csv('data/shipping_data_0.csv')
        shipping_data_1 = pd.read_csv('data/shipping_data_1.csv')
        shipping_data_2 = pd.read_csv('data/shipping_data_2.csv')

        self.populate_first_df(shipping_data_0)
        self.populate_second_df(shipping_data_1, shipping_data_2)

    def populate_first_df(self, dataframe):
        "Populate database with data from first dataframe"

        for index, row in dataframe.iterrows():
            # extract required data
            product_name = row['product']
            product_quantity = row['product_quantity']
            destination = row['destination_store']
            origin = row['origin_warehouse']

            # insert data into database
            self.insert_product(product_name)
            self.insert_shipment(product_name, product_quantity, origin, destination)

    def populate_second_df(self, df_1, df_2):
        "Populate database with data from second and third dataframe"

        # extract shipment information
        shipment_info = {}
        for index, row in df_2.iterrows():
            shipment_id = row['shipment_identifier']
            origin = row['origin_warehouse']
            destination = row['destination_store']

            shipment_info[shipment_id] = {
                "origin": origin,
                "destination": destination,
                "products": {}
            }

        # extract product information
        for index, row in df_1.iterrows():
            shipment_id = row['shipment_identifier']
            product_name = row['product']

            # Populate 'Shipment_info'
            products = shipment_info[shipment_id]["products"]
            if product_name not in products:
                products[product_name] = 1
            else:
                products[product_name] += 1

        # insert data into database
        for shipment_id, shipment in shipment_info.items():
            origin = shipment['origin']
            destination = shipment['destination']

            for product_name, product_quantity in shipment["products"].items():
                # insert data into database
                self.insert_product(product_name)
                self.insert_shipment(product_name, product_quantity, origin, destination)

    def insert_product(self, product_name):
        self.cursor.execute("INSERT OR IGNORE INTO product(name) VALUES (?);", (product_name,))
        self.connection.commit()

    def insert_shipment(self, product_name, product_quantity, origin, destination):
        # Retrieve productID
        self.cursor.execute("SELECT id FROM product WHERE name = ?;", (product_name,))
        product_id = self.cursor.fetchone()[0]

        self.cursor.execute("""INSERT OR IGNORE INTO shipment (product_id, quantity, origin, destination)
                                VALUES (?, ?, ?, ?);""", (product_id, product_quantity, origin, destination))
        self.connection.commit()

    def close(self):
        self.connection.close()

if __name__ == "__main__":
    solution = Solution("shipment_database.db")
    solution.read_and_populate()
    solution.close()




