from dotenv import dotenv_values
from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi

uri = "localhost"
port = 27017
db_name = "storedist"
# Create a new client and connect to the server
print("Connecting to the database")
client = MongoClient(uri, port)
print("Connected to the database")

db = client[db_name]
# Send a ping to confirm a successful connectiontry:
try:
    print("Users:", db["User"].count_documents({}))
    print("Activities:", db["Activity"].count_documents({}))
    print("TrackPoints:", db["TrackPoint"].count_documents({}))

except Exception as e:
    print("ERROR: Failed to connect to db:", e)

# Close the connection
client.close()
print("closed")
