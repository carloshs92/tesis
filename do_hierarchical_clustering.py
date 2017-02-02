import Orange
import csv
import json
from pymongo import MongoClient
import datetime

def fnGetClusters(searchs):
  data = Orange.data.Table(searchs)
  dist_matrix = Orange.distance.Euclidean(data)
  hierar = Orange.clustering.hierarchical.HierarchicalClustering(n_clusters=10)
  hierar.linkage = Orange.clustering.hierarchical.AVERAGE
  hierar.fit(dist_matrix)
  tags = list()
  for label in hierar.labels:
    tags.append(str(label).replace('.0', ''))
  return tags

def fnAddClusters(clusters, searchs):
  users = list()
  with open(searchs, 'rt', encoding="utf8") as file:
    users_searchs = csv.reader(file, delimiter=',')
    counter = 0
    for row in users_searchs:
      if not row[0].isdigit():
        row.append('cluster')
        users.append(row)
        continue
      row.append(str(clusters[counter]))
      users.append(row)
      counter += 1
  return users

def fnExportSegments(users, segments):
  file = open(segments, 'w')
  for user in users:
    file.write(",".join(user))
    file.write("\n")
  file.close()
  return

def fnCreateCriterios(segments, clusters):
  criterio = dict()
  for cluster in clusters:
    cluster = str(cluster)
    if not cluster in criterio:
      criterio[cluster] = dict()
      criterio[cluster]['times'] = 0
      criterio[cluster]['cantidad'] = 0
      criterio[cluster]['fec_creacion'] = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    criterio[cluster]['times'] += 1
  with open(segments, 'rt', encoding="utf8") as file:
    users_searchs = csv.reader(file, delimiter=',')
    header = list()
    for row in users_searchs:
      if not row[0].isdigit():
        header = row
        continue
      counter = 0
      index = len(row) - 1
      current_cluster = row[index]
      for value in row:
        if counter == 0:
          counter += 1                
          continue
        if counter == index:
          counter += 1                
          continue
        if int(value) != 0:
          key = str(header[counter])
          if not key in criterio[current_cluster]:
            criterio[current_cluster][key] = int(value)
          else:
            criterio[current_cluster][key] += int(value)
          criterio[current_cluster]['cantidad'] += int(value)
        counter += 1
  return criterio

def fnExportCriterios(criterios, criterio_segmento):
  not_values = ['times', 'cantidad', 'fec_creacion']
  for criterio in criterios:
    values = criterios[criterio]
    for value in values:
      if value in not_values:
        continue
      criterios[criterio][value] = int(criterios[criterio][value])/int(criterios[criterio]['cantidad'])
  connect = MongoClient("mongodb://localhost:27017")

  criterio_json = json.dumps(criterios)
  criterio_file = open(criterio_segmento, 'w')
  criterio_file.write(criterio_json)
  criterio_file.close()
  
  db = connect['aptitus']
  db.criterio_segmento.insert_one(criterios)

def init():
  searchs = 'resources/searchs.csv'
  segments = 'resources/segmento_postulante.csv'
  criterio_segmento = 'resources/criterio_segmento.js'

  clusters = fnGetClusters(searchs)
  # Mezclar los usuarios con los clusters
  users = fnAddClusters(clusters, searchs)
  # Guardar los segmentos
  fnExportSegments(users, segments)
  criterios = fnCreateCriterios(segments, clusters)
  fnExportCriterios(criterios, criterio_segmento)
init()

#print(type(hierar.labels))
#print(hierar.labels)
#for label in hierar.labels:
#  print(label)