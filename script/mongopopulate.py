import random
import time
import sys
import pymongo
import pyprind

start = time.time()

def main():
  #open Mongo connection
  conn = pymongo.MongoClient('localhost', 27017)
  db = conn["null"]

  for i in xrange(1000):
  	print "Collection number %d" % i
  	col = db["collect%d" % i]
  	for j in pyprind.prog_bar(xrange(random.randrange(1,15000))):
  		ins = { "name": "John%d" % j, "address": "Highway %d" % j }
  		x = col.insert_one(ins)

  end = time.time()
  conn.close()	
  print "Elapsed time: %i seconds" % (end - start)

main()