from dotenv import dotenv_values
from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi


class DbConnector:
    def __init__(self):
        db_name = "storedist"
        uri = "localhost"
        port = 27017

        # Connect to the database

        try:
            self.client = MongoClient(uri, port)
            self.db = self.client[db_name]
            print("\n-----------------------------------------------")
            print("Connected to the database")

        except Exception as e:
            print("ERROR: Failed to connect to db:", e)

    def close_connection(self):
        # close the cursor
        self.client.close()
        print("\n-----------------------------------------------")
        print("Connection to is closed")


if __name__ == "__main__":
    db = DbConnector()
    db.close_connection()
