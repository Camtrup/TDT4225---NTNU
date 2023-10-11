from dotenv import dotenv_values
from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi

config = dotenv_values(".env")

uri = config["ATLAS_URI"]
# Create a new client and connect to the server
client = MongoClient(uri, server_api=ServerApi("1"))

db = client[config["DB_NAME"]]
# Send a ping to confirm a successful connectiontry:

try:
    a = db["user"].find()
    b = db.list_collection_names()
    print(b)

    for i in a:
        print(i)
except:
    print("error")


# Close the connection
client.close()
print("closed")
