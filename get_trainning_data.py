from pymongo import MongoClient
from pymongo import DESCENDING

def fnConnectMongo():
  db = MongoClient("mongodb://localhost:27017")
  return db

def fnGetPostulations(mongo):
  db = mongo['aptitus']
  tb_avisos = db['avisos']
  postulations = tb_avisos.find({ "auth":  {"$exists" : "true", "$ne" : ""}}).sort([("fecha_hora", DESCENDING)]).limit(1000000)
  return postulations

def fnCleanData(postulations):
  data = dict()
  users = dict()
  aviso = dict()
  for postulation in postulations:
    print(postulation)
  return data

def init():
  print("Downloading Searchs...")
  # Connect Mongo and MySQL
  mongo = fnConnectMongo()
  # Get Postulations DataBank
  postulations = fnGetPostulations(mongo)
  # Clean Data
  data = fnCleanData(postulations)
  # Export Data
  
  return

init()           
