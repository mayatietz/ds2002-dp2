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
count = 0

collection.drop()

for filename in os.listdir(directory):
  with open(os.path.join(directory, filename)) as f:
   
    # loading the json file
    try:
      file_data = json.load(f)
    except Exception as e:
      print(e, "error when loading", f)

    # create python dictionary
    d = {}
    for item in file_data:
      #for num in item:
          _id = item["_id"]
          if _id in d:
                 d[_id].append(item)
          else:
                 d[_id] = [item]
    
# Inserting the loaded data in the collection
# if JSON contains data more than one entry
# insert_many is used else insert_one is used
    if isinstance(d, list):
      try:
        collection.insert_many(d, ordered=False)
        count = count + 1
      except Exception as e:
        print(e, "ERROR when importing MANY into Mongo")
    else:
      try:
        collection.insert_one(d)
        count = count + 1
      except Exception as e:
        print(e, "ERROR when importing ONE into Mongo")

print(count)
print(collection.count_documents({}))

