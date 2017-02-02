from pymongo import MongoClient
from pymongo import DESCENDING
import _mysql


def fnConnectMongo():
  db = MongoClient("mongodb://localhost:27017")
  return db

def fnConnectMySQL():
  db = _mysql.connect("localhost", "root", "mysql", "database")
  return db

def fnGetPostulantsSearchs(mongo):
  db = mongo['aptitus']
  tb_busqueda = db['busquedas']
  # Deberia hacerlo solo con las busquedas del dia (http://stackoverflow.com/questions/26366417/how-to-make-a-query-date-in-mongodb-using-pymongo)
  searchs = tb_busqueda.find().sort([("fecha_hora", DESCENDING)]).limit(1000000)
  return searchs

def fnCleanData(searchs):
  data = dict()
  users = dict()
  filters = list()
  for search in searchs:
    if 'busqueda' in search and search['auth'] != "":
      if not 'postulante' in search['auth']:
        continue
      id_user = search['auth']['postulante']['id_usuario']
      if id_user not in users:
        users[id_user] = dict() 
      # DATOS DE AERA
      if 'areas' in search['busqueda']:
        for area in search['busqueda']['areas']:
          key = 'areas/' + area
          if not key in users[id_user]:
            users[id_user][key] = 1
          else:
            users[id_user][key] += 1
          if not key in filters:
            filters.append(key)
        print('updating areas..')

      # DATOS DE NIVEL
      if 'nivel' in search['busqueda']:
        for nivel in search['busqueda']['nivel']:
          key = 'nivel/' + nivel
          if not key in users[id_user]:
            users[id_user][key] = 1
          else:
            users[id_user][key] += 1
          if not key in filters:
            filters.append(key)
        print('updating nivel..')

      # DATOS DE MODALIDAD
      if 'modalidad' in search['busqueda']:
        for modalidad in search['busqueda']['modalidad']:
          key = 'modalidad/' + modalidad
          if not key in users[id_user]:
            users[id_user][key] = 1
          else:
            users[id_user][key] += 1
          if not key in filters:
            filters.append(key)
        print('updating modalidad..')

      # DATOS DE DISCAPACIDAD
      if not 'discapacidad' in search['busqueda']:
        key = 'discapacidad'        
        if not key in users[id_user]:
          users[id_user][key] = 1
        else:
          users[id_user][key] += 1
        if not key in filters:
          filters.append(key)
        print('updating discapacidad..')
  data['filters'] = filters
  data['users']   = users
  return data

def fnAddUserData(users, mysql):
  #for id_user in users.keys():
    # Buscar nivel actual
    # Buscar area actual
    # Buscar experiencia
  #  print(id_user)
  return users

def fnExportData(data):
  filters = data['filters']
  users = data['users']
  file = open("resources/searchs.csv", 'w')

  # SE AGREGA CABECERA
  header_row = list()
  header_row.append('ID usuario')

  for key in filters:
    header_row.append(key)
  file.write(",".join(header_row))
  file.write("\n")

  # SE AGREGA DATA
  for id_user in users.keys():
    row = list()
    row.append(str(id_user))
    #row.append(users[id_user]['user_nivel'])
    #row.append(users[id_user]['user_area'])
    #row.append(users[id_user]['user_experience'])
    for key in filters:
      counter = 0
      if key in users[id_user]:
        counter = users[id_user][key]
      row.append(str(counter))
    file.write(",".join(row))
    file.write("\n")
  file.close()
  return

def init():
  print("Downloading Searchs...")
  # Connect Mongo and MySQL
  mongo = fnConnectMongo()
  mysql = fnConnectMySQL()
  # Get Searchs and Users
  searchs = fnGetPostulantsSearchs(mongo)
  # Clean Data
  data = fnCleanData(searchs)
  data['users'] = fnAddUserData(data['users'], mysql)
  # Export Data
  fnExportData(data)
  return

init()           
