import random
import time
import sys
import pymongo
import pyprind

start = time.time()

def main():
  #open Mongo connection
  conn = pymongo.MongoClient('localhost', 27017)
  db = conn["testdb"]
  acc = 0
  accmap = {}

  for i in conn['testdb'].list_collection_names():
    res = db.command("collstats", i)
    count = res["count"]
    accpm[i] = count
    acc += count

  end = time.time()
  conn.close()	
  print "Count: %d" % acc
  print "Elapsed time: %i seconds" % (end - start)

main()
