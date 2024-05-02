from pymongo import MongoClient, errors
from bson.json_util import dumps
import os
import json

MONGOPASS = os.getenv('MONGOPASS')
uri = "mongodb+srv://cluster0.pnxzwgz.mongodb.net/"
client = MongoClient(uri, username='nmagee', password=MONGOPASS, connectTimeoutMS=200, retryWrites=True)
# specify a database
db = client.met9krd
# specify a collection
collection = db.dp2

directory = "data"

for filename in os.listdir(directory):
  with open(os.path.join(directory, filename)) as f:
    try:
      data = json.load(f)
    except Exception as e:
      print(e, "error when loading", f)
# If the JSON data is a list of records
    if isinstance(data, list):
                # Insert each record into the collection
        for record in data:
            collection.insert_one(record)
            # If the JSON data is a single record
    elif isinstance(data, dict):
        collection.insert_one(data)
    else:
        print(f"Ignoring file {filename}: Invalid JSON format")

print("Import completed successfully!")

    # if isinstance(file_data, list):
    #   try:
    #     collection.insert_many(file_data)
    #   except Exception as e:
    #     print(e, "when importing into Mongo")
    # else:
    #   try:
    #     collection.insert_one(file_data)
    #   except Exception as e:
    #     print(e)

